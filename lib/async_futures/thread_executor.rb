# frozen_string_literal: true

require_relative 'executor'

require 'etc'
require 'monitor'
require 'set' # rubocop:disable Lint/RedundantRequireStatement

module AsyncFutures
  # `Executor` implementation based on `Thread` primitives
  # that uses a pool of up to `max_workers` to execute calls concurrently.
  class ThreadExecutor
    include Executor
    include MonitorMixin

    # Create a new `ThreadExecutor`.
    #
    # Uses a pool of up to `max_workers`
    # to execute tasks concurrently.
    # If no value is given for `max_workers`
    # it will default to `[32, Etc.nprocessors + 4].min`.
    # Workers are spawned lazily as needed
    # when tasks are added to the work queue.
    #
    # The parameter `thread_name_prefix` can be used
    # to optionally add a prefix to generated `Thread` names.
    #
    # If the `reap_after` keyword argument is given,
    # worker threads will be shut down
    # if they haven't received any work after this amount of seconds.
    # If it is `nil` or not given,
    # they will not be reaped until the `ThreadExecutor` instance is `shutdown`.
    def initialize(max_workers: nil, thread_name_prefix: '', reap_after: nil)
      super()
      @max_workers = (max_workers || [32, Etc.nprocessors + 4].min).to_i
      @thread_name_prefix = thread_name_prefix.to_s
      @reap_after = reap_after
      @tasks = Thread::Queue.new
      @pool = Set.new

      at_exit { shutdown(wait: false) }
    end

    # Asynchronously submit a task for execution.
    #
    # For `ThreadExecutor` the tasks are never run immediately upon submission.
    # They are placed into a work queue
    # to be picked up later by worker threads.
    #
    # This does _not_ guarantee
    # that any particular task will be run concurrently
    # with any other particular task;
    # that is dependent on how many worker threads and tasks there are
    # at any given point in time.
    def submit(*args, **kwargs, &block)
      raise ArgumentError.new('No block given') unless block
      raise 'ThreadExecutor instance is shutdown' if @tasks.closed?

      Future.new.tap do |future|
        @tasks.push([future, block, args, kwargs])
        maybe_spawn_worker
      end
    end

    alias submit_concurrent submit

    public :map

    # Shutdown executor.
    #
    # Can be called multiple times.
    # The block given will always be run,
    # but the actual procedure to shutdown afterward will only be called once,
    # on the first time.
    def shutdown(wait: true, cancel_futures: false, &block)
      block&.call(self)
    ensure
      unless check_set_shutdown
        if cancel_futures
          while (task = @tasks.pop)
            future = task[0]
            future.cancel
          end
        end

        if wait
          synchronize { @pool.dup }.each do |thread|
            thread.join
            synchronize { @pool.delete(thread) }
          end
        end
      end
    end

    private

    # Returns the current shutdown state,
    # then always sets shutdown to `true` no matter what its current value is.
    # This is all done atomically.
    def check_set_shutdown
      synchronize do
        return true if @tasks.closed?

        @tasks.close
        return false
      end
    end

    # Only spawn a worker if one is needed.
    def maybe_spawn_worker
      # synchronize when interacting directly with @pool
      spawn_worker if synchronize { @pool.empty? } || (@tasks.size > 1 && synchronize { @pool.size } < @max_workers)
    end

    # Always spawn a worker
    def spawn_worker # rubocop:disable Metrics/AbcSize
      thread = Thread.new do
        Thread.current.name = "#{@thread_name_prefix}_#{Thread.current.object_id}" unless @thread_name_prefix.empty?

        while (task = @tasks.pop(timeout: @reap_after))
          tfuture, tblock, targs, tkwargs = task

          next unless tfuture.set_running_or_notify_cancel

          begin
            result = tblock.call(*targs, **tkwargs)
          rescue Exception => e # rubocop:disable Lint/RescueException
            tfuture.set_exception(e)
          else
            tfuture.set_result(result)
          end
        end
      ensure
        synchronize { @pool.delete Thread.current }
      end
      synchronize { @pool.add thread }
    end
  end
end
