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
    def initialize(max_workers: nil, worker_name_prefix: nil, reap_after: nil)
      @max_workers = (max_workers || [32, Etc.nprocessors + 4].min).to_i
      @worker_name_prefix = worker_name_prefix
      @reap_after = reap_after
      @mutex = Thread::Mutex.new
      @tasks = Thread::Queue.new
      @pool = Set.new
      @worker_count = 0

      at_exit { shutdown(wait: false) }
    end

    # Asynchronously submit a task for execution.
    #
    # See `AsyncFutures::Executor.submit` method for full documentation.
    def submit(*args, **kwargs, &block)
      raise ArgumentError.new('No block given') unless block

      Future.new.tap do |future|
        @tasks.push([future, block, args, kwargs])
        maybe_spawn_worker
      rescue ClosedQueueError
        raise 'ThreadExecutor instance is shutdown'
      end
    end

    alias submit_concurrent submit

    public :map

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
    def spawn_worker
      thread = Thread.new do
        Thread.current.name = new_worker_name
        while (task = @tasks.pop(timeout: @reap_after))
          tfuture, tblock, targs, tkwargs = task

          tfuture.complete(*targs, **tkwargs, &tblock)
        end
      ensure
        synchronize { @pool.delete Thread.current }
      end
      synchronize { @pool.add thread }
    end
  end
end
