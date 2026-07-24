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
  # For `ThreadExecutor`, with no arguments passed,
  # the tasks are not run immediately upon submission.
  # They are placed into a work queue
  # to be picked up later by worker threads.
  #
  # This does _not_ guarantee
  # that any particular task will be run concurrently
  # with any other particular task;
  # that is dependent on how many worker threads and tasks there are
  # at any given point in time
  # and whether the `strict_concurrency` argument is passed.
  class ThreadExecutor # rubocop:disable Metrics/ClassLength
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
    # The `strict_concurrency` argument
    # changes the behavior of both `submit` and `submit_concurrent`.
    # When the argument is `false`
    # then both methods will just put tasks on a queue
    # and assume they will get picked up later.
    # When the argument is `true` then the following happens:
    #
    # - `submit`: if adding another task to the queue
    #   would make more tasks than available (or potential) workers,
    #   then the task is run immediately
    #   and a completed future is returned to the caller.
    # - `submit_concurrent`: if adding another task to the queue
    #   would make more tasks than available (or potential) workers,
    #   then a `NoConcurrencyError` is raised.
    #
    # With `strict_concurrency: false`
    # you can do interesting/dangerous things.
    # For example, you can add tasks to the executor
    # from within a executor worker thread,
    # even if the max worker count is only `1`.
    # If you think through this scenario
    # you will realize that joining on the returned future
    # will deadlock the worker thread.
    #
    # This defaults to `false` precisely because
    # scenarios like this are uncommon.
    # The most common scenario is firing of many tasks
    # from the main thread of execution
    # that do not interact other than to return a value
    # to the main thread.
    #
    # A scenario where you might want `strict_concurrency` to be `true`:
    # you have client and server tasks
    # and they *must* run concurrent to each other in order to work correctly.
    #
    # Consider this pseudocode, for example:
    #
    # ```ruby
    # ThreadExecutor.new(max_workers: 1, strict_concurrency: true).shutdown do |executor|
    #   # ... other work, potentially using executor ...
    #
    #   # This can fail if all workers are busy. Good! We want it to.
    #   # It doesn't make sense to run the client code afterward
    #   # if the server isn't first running concurrently.
    #   executor.submit_concurrent { Server.new.listen() }
    #
    #   # The client doesn't *need* to run concurrently;
    #   # It is logically correct to run it either concurrently OR immediately,
    #   # so we use `submit` instead of `submit_concurrent` for client code.
    #   executor.submit do
    #     client = Client.new
    #     client.ping()
    #     # ... interact with `client` while server runs in background concurrently ...
    #   ensure
    #     client.signal_server_shutdown()
    #   end
    #
    #   # ... maybe more code after ...
    # end
    # ```
    #
    # If the `reap_after` keyword argument is given,
    # worker threads will be shut down
    # if they haven't received any work after this amount of seconds.
    # If it is `nil` or not given,
    # they will not be reaped until the `ThreadExecutor` instance is `shutdown`.
    #
    # The parameter `worker_name_prefix` can be used
    # to optionally add a prefix to generated worker names.
    def initialize(
      max_workers: nil,
      strict_concurrency: false,
      reap_after: nil,
      worker_name_prefix: nil
    )
      @max_workers = (max_workers || [32, Etc.nprocessors + 4].min).to_i
      @strict_concurrency = strict_concurrency
      @reap_after = reap_after
      @worker_name_prefix = worker_name_prefix
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

      Future.new.tap do |f|
        f.complete(*args, **kwargs, &block) unless queue_task(f, *args, **kwargs, &block)
      rescue ClosedQueueError
        raise 'ThreadExecutor instance is shutdown'
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

      Future.new.tap do |f|
        raise NoConcurrencyError.new('Tasks exceed potential workers') unless queue_task(f, *args, **kwargs, &block)
      rescue ClosedQueueError
        raise 'ThreadExecutor instance is shutdown'
      end
    end

    # Return the current size of the worker pool
    def pool_size
      synchronize { @pool.size }
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
    def shutdown(wait: true, cancel_futures: false)
      yield(self) if block_given?
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

    # Attempt to queue task.
    # Return `true` if successful, `false` otherwise.
    #
    # Raises `ClosedQueueError` if the task queue is closed.
    #
    # If `@strict_concurrency` is `false`,
    # this method always queues the task.
    #
    # If `@strict_concurrency` is `true`,
    # task may or may not be queued
    # based on whether there are any potentially available workers.
    #
    # May spawn a new worker, if the task was queued.
    def queue_task(future, *args, **kwargs, &block)
      queued = if @strict_concurrency
                 synchronize do
                   potential_workers = (@max_workers - @pool.size) + @pool.values.count(&:!)
                   if (@tasks.size + 1) <= potential_workers
                     @tasks.push([future, block, args, kwargs])
                     true
                   else
                     raise ClosedQueueError if @tasks.closed?

                     false
                   end
                 end
               else
                 @tasks.push([future, block, args, kwargs])
                 true
               end

      queued.tap { maybe_spawn_worker if queued }
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
        AsyncFutures.worker_name = new_worker_name
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
