# frozen_string_literal: true

require_relative 'executor'

require 'etc'
require 'set' # rubocop:disable Lint/RedundantRequireStatement

module AsyncFutures
  # `Executor` implementation based on `Thread` primitives
  # that uses a pool of up to `max_workers` to execute calls concurrently.
  #
  # `ThreadExecutor` specific submission considerations:
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
  class ThreadExecutor
    include Executor

    # Create a new `ThreadExecutor`.
    #
    # Uses a pool of up to `max_workers`
    # to execute tasks concurrently.
    # If no value is given for `max_workers`
    # it will default to `[32, Etc.nprocessors + 4].min`.
    # Workers are spawned lazily as needed
    # when tasks are added to the work queue.
    #
    # The parameter `worker_name_prefix` can be used
    # to optionally add a prefix to generated `Thread` names.
    #
    # If the `reap_after` keyword argument is given,
    # worker threads will be shut down
    # if they haven't received any work after this amount of seconds.
    # If it is `nil` or not given,
    # they will not be reaped until the `ThreadExecutor` instance is `shutdown`.
    #
    # If the `strict_concurrency` keyword argument is given
    # and it is not falsy
    # it will cause `submit_concurrent` to raise `NoConcurrencyError`
    # if it is not possible to run all pending tasks
    # plus the newly submitted task with full concurrency.
    # If it is falsy (the default)
    # then it is considered to have _loose_ concurrency
    # (it is considered concurrent only with the submitting thread).
    # It should never raise `NoConcurrencyError`.
    def initialize(max_workers: nil, worker_name_prefix: nil, reap_after: nil, strict_concurrency: false)
      @max_workers = (max_workers || [32, Etc.nprocessors + 4].min).to_i
      @worker_name_prefix = worker_name_prefix
      @reap_after = reap_after
      @strict_concurrency = strict_concurrency
      @mutex = Thread::Mutex.new
      @tasks = Thread::Queue.new

      # Set Hash value to `true` when a worker is running
      # and `false` otherwise.
      @pool = {}
      @worker_count = 0

      at_exit { shutdown(wait: false) }
    end

    # Asynchronously submit a task for execution.
    #
    # May run task immediately
    # and return a completed `Future`
    # under certain circumstances.
    #
    # See `AsyncFutures::Executor.submit` method for full documentation.
    def submit(*args, **kwargs, &block)
      raise ArgumentError.new('No block given') unless block

      shutdown_msg = 'ThreadExecutor instance is shutdown'

      Future.new.tap do |future|
        if synchronize { @pool.include?(Thread.current) } && @max_workers == 1
          raise shutdown_msg if @tasks.closed?

          future.complete(*args, **kwargs, &block)
        else
          @tasks.push([future, block, args, kwargs])
          maybe_spawn_worker
        end
      rescue ClosedQueueError
        raise shutdown_msg
      end
    end

    # Submit a task for concurrent execution.
    #
    # Will raise `NoConcurrencyError`
    # if it is not possible
    # to run the task concurrently
    # with other already scheduled tasks.
    #
    # See `AsyncFutures::Executor.submit_concurrent` method for full documentation.
    def submit_concurrent(*args, **kwargs, &block)
      raise ArgumentError.new('No block given') unless block

      shutdown_msg = 'ThreadExecutor instance is shutdown'

      Future.new.tap do |future|
        if synchronize { @pool.include?(Thread.current) } && @max_workers == 1
          raise shutdown_msg if @tasks.closed?

          raise NoConcurrencyError.new('Task submitted from lone worker thread is not concurrent')
        end
        @tasks.push([future, block, args, kwargs])
        maybe_spawn_worker
      rescue ClosedQueueError
        raise shutdown_msg
      end
    end

    # :nocov:

    # Always returns `true`
    # for `ThreadExecutor`.
    def support_concurrency?
      true
    end

    # :nocov:

    # Shutdown `ThreadExecutor` instance.
    #
    # See `AsyncFutures::Executor.shutdown` for full documentation.
    def shutdown(wait: true, cancel_futures: false, &block)
      block&.call(self)
    ensure
      unless check_and_set_shutdown!
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

    def synchronize(&)
      @mutex.synchronize(&)
    end

    # Returns the current shutdown state,
    # then sets internal shutdown state to `true`.
    # This is all done atomically to avoid race conditions.
    def check_and_set_shutdown!
      synchronize do
        return true if @tasks.closed?

        @tasks.close
        return false
      end
    end

    # Only spawn a worker if one is needed.
    def maybe_spawn_worker
      # synchronize when interacting directly with @pool
      spawn_worker if !@tasks.empty? && synchronize { @pool.size } < @max_workers
    end

    def new_worker_name
      synchronize do
        if @worker_name_prefix
          "#{@worker_name_prefix}_#{@worker_count += 1}"
        else
          "#{self.class.name}_#{object_id}_worker_#{@worker_count += 1}"
        end
      end
    end

    # Always spawn a worker
    def spawn_worker # rubocop:disable Metrics/AbcSize
      thread = Thread.new do
        Thread.current.name = new_worker_name
        while (task = @tasks.pop(timeout: @reap_after))
          synchronize { @pool[Thread.current] = true }

          tfuture, tblock, targs, tkwargs = task
          tfuture.complete(*targs, **tkwargs, &tblock)

          synchronize { @pool[Thread.current] = false }
        end
      ensure
        synchronize { @pool.delete Thread.current }
      end
      synchronize { @pool[thread] ||= false }
    end
  end
end
