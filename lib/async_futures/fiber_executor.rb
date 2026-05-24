# frozen_string_literal: true

require_relative 'error'
require_relative 'executor'

require 'set' # rubocop:disable Lint/RedundantRequireStatement

module AsyncFutures
  # `Executor` implementation based on `Fiber` primitives.
  # Requires that a `Fiber::Scheduler` be set for `Fiber.scheduler`
  # in order to work.
  #
  # Several benefits of using `FiberExecutor` over using `Fiber.schedule` directly:
  #
  # - By default `Fiber` instances run via `Fiber.schedule`
  #   have no straightforward way of returning their final result
  #   upon completion
  #   like `Thread` and `Ractor` do
  #   (both support calling `value` to get the final result).
  #   `Future` makes this trivial to do for a scheduled `Fiber`.
  # - `Fiber` instances cannot currently be shared across `Thread` instances,
  #   though this may change someday.
  #   However, `Future` instances can safely be shared
  #   across both `Threads` and `Fibers`
  #   (Ractors can share neither `Fiber` nor `Future`
  #   and that is unlikely to ever change).
  class FiberExecutor
    include Executor
    include MonitorMixin

    # Create a new `FiberExecutor`.
    #
    # Spawns fibers via `Fiber.schedule`.
    #
    # Raises `AsyncFutures::Error`
    # unless `Fiber.scheduler` is set.
    #
    # Because auto-fibers do not yield control
    # unless they encounter a blocking operation,
    # it is completely possible
    # that the `Fiber` runs to completion upon submission.
    # Thus, `submit_concurrent` fails by default
    # unless the parameter `treat_as_concurrent` is set to `true`.
    #
    # All internal state is protected via mutex,
    # so it is safe to use a single `FiberExecutor` instance across multiple threads.
    # However each thread must have its own `Fiber::Scheduler` set
    # in order to successfully call `submit`.
    def initialize(treat_as_concurrent: false)
      raise Error.new('No Fiber.scheduler set') unless Fiber.scheduler

      super()
      @treat_as_concurrent = treat_as_concurrent
      @is_shutdown = false
      @futures = Set.new

      at_exit { shutdown(wait: false) }
    end

    # Asynchronously submit a task for execution.
    #
    # For `FiberExecutor` the tasks are run immediately upon submission
    # using the `Fiber.schedule` method.
    # This method will return
    # as soon as the Fiber hits a blocking operation
    # (and thus yields control back to the scheduling Fiber)
    # or runs the `Fiber` to completion.
    # Thus it is completely possible
    # that the returned `Future` is already completed
    # by the time it is returned to the caller.
    #
    # The `FiberExecutor` implementation does _not_ guarantee
    # that any particular task will be run concurrently
    # with any other particular task;
    # that is dependent
    # on whether the submitted procs/blocks
    # have blocking operations that yield control
    # back to the `Fiber::Scheduler`
    # and whether the `Fiber::Scheduler` properly implements
    # `Fiber` switching for those operations.
    def submit(*args, **kwargs, &block)
      raise ArgumentError.new('No block given') unless block
      raise 'FiberExecutor instance is shutdown' if synchronize { @is_shutdown }

      Future.new.tap do |future|
        synchronize { @futures.add future }

        Fiber.schedule do
          # `sleep(0)` to attempt to force the scheduled Fiber
          # to immediately return control to the scheduler.
          # Because this is dependent on the scheduler implementation,
          # this is not guaranteed to work.
          sleep(0)

          break unless future.set_running_or_notify_cancel

          begin
            result = block.call(*args, **kwargs)
          rescue Exception => e # rubocop:disable Lint/RescueException
            future.set_exception(e)
          else
            future.set_result(result)
          end
        ensure
          synchronize { @futures.delete future }
        end
      end
    end

    # Raises `NoConcurrencyError`
    # unless `treat_as_concurrent` was passed as `true`
    # to the `FiberExecutor` constructor.
    #
    # Otherwise,
    # forwards to `submit` method.
    def submit_concurrent(...)
      raise NoConcurrencyError unless @treat_as_concurrent

      submit(...)
    end

    public :map

    # Shutdown executor.
    #
    # Can be called multiple times.
    # The block given will always be run,
    # but the actual procedure to shutdown afterward will only be called once,
    # on the first time.
    def shutdown(wait: true, cancel_futures: false, &block) # rubocop:disable Metrics/CyclomaticComplexity,Metrics/PerceivedComplexity
      block&.call(self)
    ensure
      unless check_set_shutdown
        futures_dup = synchronize { @futures.dup } if wait || cancel_futures
        futures_dup.reject!(&:cancel) if cancel_futures
        futures_dup.reject!(&:join) if wait
        synchronize { @futures.replace(@futures & futures_dup) } if wait || cancel_futures
      end
    end

    private

    # Returns the current shutdown state,
    # then always sets shutdown to `true` no matter what its current value is.
    # This is all done atomically.
    def check_set_shutdown
      synchronize do
        return true if @is_shutdown

        @is_shutdown = true
        return false
      end
    end
  end
end
