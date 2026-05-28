# frozen_string_literal: true

require_relative 'executor'

require 'etc'
require 'set' # rubocop:disable Lint/RedundantRequireStatement

module AsyncFutures
  # `Executor` implementation based on Process forking
  # that uses up to `max_workers` to execute calls concurrently.
  #
  # `ProcessExecutor` specific submission considerations:
  #
  # For `ProcessExecutor` the tasks are never run immediately upon submission.
  # They are placed into a work queue
  # to be picked up later.
  #
  # Process workers are not reused for work.
  # Each task gets a freshly forked process.
  # This is because marshalling anonymous blocks is not trivial;
  # it is simpler to just fork after the block closure has been defined.
  # Use `ThreadExecutor` or `RactorExecutor`
  # for `Executor` implementations that support worker reuse.
  #
  # Consequently, this executor is only really useful for expensive calculations
  # where the startup time for a process
  # is dwarfed by the time needed for the actual work.
  # If RactorExecutor is available on your Ruby version
  # it is almost certainly a better choice than this.
  #
  # This does _not_ guarantee
  # that any particular task will be run concurrently
  # with any other particular task;
  # that is dependent on how many worker threads and tasks there are
  # at any given point in time.
  class ProcessExecutor
    include Executor

    # Create a new `ProcessExecutor`.
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
    # they will not be reaped until the `ProcessExecutor` instance is `shutdown`.
    def initialize(max_workers: nil, worker_name_prefix: '', reap_after: nil)
      @max_workers = (max_workers || [32, Etc.nprocessors + 4].min).to_i
      @worker_name_prefix = worker_name_prefix.to_s
      @reap_after = reap_after
      @mutex = Thread::Mutex.new
      @tasks = Thread::Queue.new
      @pool = Set.new

      at_exit { shutdown(wait: false) }
    end

    # Asynchronously submit a task for execution.
    #
    # See `AsyncFutures::Executor.submit` method for full documentation.
    def submit(*args, **kwargs, &block)
      raise ArgumentError.new('No block given') unless block
      raise 'ProcessExecutor instance is shutdown' if @tasks.closed?

      Future.new.tap do |future|
        @tasks.push([future, block, args, kwargs])
        maybe_spawn_worker
      end
    end

    alias submit_concurrent submit

    public :map

    # Shutdown `ProcessExecutor` instance.
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

    # Always spawn a worker
    def spawn_worker # rubocop:disable Metrics/AbcSize
      thread = Thread.new do
        Thread.current.name = "#{@worker_name_prefix}_#{Thread.current.object_id}" unless @worker_name_prefix.empty?

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
