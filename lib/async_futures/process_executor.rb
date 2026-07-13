# frozen_string_literal: true

require_relative 'executor'

require 'etc'
require 'set' # rubocop:disable Lint/RedundantRequireStatement
require 'json'
require 'json/add/exception'

module AsyncFutures
  # `Executor` implementation based on Process forking
  # that uses up to `max_workers` to execute calls in parallel.
  #
  # `ProcessExecutor` specific submission considerations:
  #
  # For `ProcessExecutor` the tasks are never run immediately upon submission.
  # They are placed into a work queue
  # to be picked up later.
  #
  # Process workers are not reused for work
  # loke Threads and Ractors are.
  # Each task gets a freshly forked process.
  # This is because marshalling anonymous blocks is not trivial in Ruby;
  # it is simpler to just fork after the block closure has been defined.
  # Use `ThreadExecutor` or `RactorExecutor`
  # for `Executor` implementations that support worker reuse.
  #
  # Consequently, this executor is only really useful for expensive calculations
  # where the startup time for a process
  # is dwarfed by the time needed for the actual work.
  # If `RactorExecutor` is available on your Ruby engine/version
  # it is almost certainly a better choice for parallel work.
  #
  # This does _not_ guarantee
  # that any particular task will be run concurrently
  # with any other particular task;
  # that is dependent on how many workers and tasks there are
  # at any given point in time.
  class ProcessExecutor # rubocop:disable Metrics/ClassLength
    include Executor

    # Create a new `ProcessExecutor`.
    #
    # Uses a pool of up to `max_workers`
    # to execute tasks in parallel.
    # If no value is given for `max_workers`
    # it will default to `[32, Etc.nprocessors + 4].min`.
    # Workers are spawned lazily as needed
    # when tasks are added to the work queue.
    #
    # The parameter `worker_name_prefix` can be used
    # to optionally add a prefix to generated `Thread` names.
    #
    # If the `move_result` keyword argument is `true`,
    # results from worker ractors will be moved instead of copied.
    # Moving is faster than copying,
    # but less safe
    # if the worker ractor keeps the values around for some reason
    # (in a cache, for example).
    # If you aren't doing something like caching inside workers
    # you are probably safe to set this to `true`.
    #
    # If the `move_args` keyword argument is `true`,
    # `args` and `kwargs` will be moved instead of copied
    # from the submitting ractor to the worker ractors.
    # Moving is faster than copying,
    # but is even less safe than `move_result`
    # because the submitting ractor
    # is more likely to have kept references to the submitted values.
    # You should only set this to `true`
    # if you are absolutely certain that submitted values
    # have no remaining references in the submitting ractor
    # otherwise the submitting ractor will error when accessing them later.
    def initialize(
      max_workers: nil,
      worker_name_prefix: nil
    )
      @max_workers = (max_workers || [32, Etc.nprocessors + 4].min).to_i
      @worker_name_prefix = worker_name_prefix

      @mutex = Thread::Mutex.new
      @condition = Thread::ConditionVariable.new
      @tasks = Thread::Queue.new

      # All private variables after this point
      # require synchronization to safely interact with.
      @futures = {}

      @pool = Set.new
      @worker_count = 0

      @task_feeder = nil
      @result_feeder = nil

      # The inter-thread communication between these is necessary for shutdown,
      # so even if nothing is submitted, we still need these to exist for now.
      maybe_spawn_task_feeder
      maybe_spawn_result_feeder

      at_exit { shutdown(wait: false) }
    end

    # Asynchronously submit a task for execution.
    #
    # See `AsyncFutures::Executor.submit` method for full documentation.
    def submit(*args, **kwargs, &block)
      raise ArgumentError.new('No block given') unless block

      Future.new.tap do |future|
        task_ary = [future, block, args, kwargs]

        @tasks.push(task_ary)
        maybe_spawn_task_feeder
        maybe_spawn_result_feeder
      rescue ClosedQueueError
        raise 'ProcessExecutor instance is shutdown'
      end
    end

    # :nocov:

    # Always returns `true`
    # for `ProcessExecutor`.
    def support_concurrency?
      true
    end

    def pool_size
      synchronize { @pool.size }
    end

    # :nocov:

    # Shutdown `ProcessExecutor` instance.
    #
    # See `AsyncFutures::Executor.shutdown` for full documentation.
    def shutdown(wait: true, cancel_futures: false, &block) # rubocop:disable Metrics/CyclomaticComplexity
      block&.call(self)
    ensure
      unless check_and_set_shutdown!
        if cancel_futures
          while (task = @tasks.pop)
            future = task[0]
            future.cancel
          end
        end

        synchronize { wait_until { @pool.empty? && @tasks.closed? && @tasks.empty? } } if wait
      end
    end

    private

    def synchronize(&block)
      @mutex.synchronize do
        block.call
      ensure
        @condition.broadcast
      end
    end

    def wait_until
      @condition.wait(@mutex) until yield
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

    def new_worker_name
      if @worker_name_prefix
        "#{@worker_name_prefix}_#{@worker_count += 1}"
      else
        "#{self.class.name}_#{object_id}_worker_#{@worker_count += 1}"
      end
    end

    def maybe_spawn_task_feeder
      synchronize { spawn_task_feeder unless @task_feeder }
    end

    def spawn_task_feeder # rubocop:disable Metrics/AbcSize
      @task_feeder = Thread.new("task_feeder_#{object_id}") do |feeder_name|
        Thread.current.name = feeder_name

        while (task = @tasks.pop)
          future, block, args, kwargs = task

          next unless future.set_running_or_notify_cancel

          future_object_id = future.object_id

          write_pipe = synchronize do
            wait_until { @pool.size <= @max_workers }

            read_pipe, write_pipe = IO.pipe
            @pool.add(read_pipe)
            @futures[future_object_id] = future
            write_pipe
          end

          Kernel.fork do
            result = block.call(*args, **kwargs)
            marshalled = Marshal.dump(result)
            json_result = JSON.dump([future_object_id, :result, marshalled])
          rescue Exception => e # rubocop:disable Lint/RescueException
            json_exc = JSON.dump([future_object_id, :exception, e.as_json])
            write_pipe.write(json_exc)
          else
            write_pipe.write(json_result)
          ensure
            write_pipe.close
          end
        end
      ensure
        synchronize do
          @task_feeder = nil
        end
      end
    end

    def maybe_spawn_result_feeder
      synchronize { spawn_result_feeder unless @result_feeder }
    end

    def spawn_result_feeder # rubocop:disable Metrics/AbcSize,Metrics/CyclomaticComplexity,Metrics/PerceivedComplexity
      @result_feeder = Thread.new("result_feeder_#{object_id}") do |feeder_name|
        Thread.current.name = feeder_name

        loop do
          break_loop, results_pipes = synchronize do
            wait_until { !@pool.empty? || (@pool.empty? && @tasks.closed? && @tasks.empty?) }

            [@pool.empty? && @tasks.closed? && @tasks.empty?, @pool.dup]
          end

          break if break_loop

          ready_pipes = results_pipes.filter { |p| p.wait_readable(0.0001) }

          next if ready_pipes.empty?

          next_pipe = ready_pipes.first
          synchronize { @pool.delete(next_pipe) }

          msg_raw = next_pipe.read
          next_pipe.close

          msg = JSON.parse(msg_raw)
          future_id, type, value = msg
          future = synchronize { @futures.delete(future_id) { raise "future_id not found #{future_id}" } }

          if type.equal? :exception
            exc_value = Exception.json_create(value)
            future.set_exception(exc_value)
          end

          if type.equal? :result
            result_value = Marshal.load(value) # rubocop:disable Security/MarshalLoad
            future.set_result(result_value)
          end
        end
      ensure
        synchronize do
          @result_feeder = nil
        end
      end
    end
  end
end
