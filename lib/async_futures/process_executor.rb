# frozen_string_literal: true

require_relative 'executor'

require 'etc'
require 'set' # rubocop:disable Lint/RedundantRequireStatement
require 'json'
require 'base64'

# :nocov:
raise LoadError.new('ProcessExecutor requires `Process.fork`') unless Process.respond_to?(:fork)
# :nocov:

module AsyncFutures
  # `Executor` implementation based on Process forking
  # that uses up to `max_workers` to execute calls in parallel.
  #
  # `ProcessExecutor` specific considerations:
  #
  # The `ProcessExecutor` class is not required by default
  # when loading the overall `AsyncFutures` gem.
  #
  # ```ruby
  # # ProcessExecutor *NOT* loaded
  # require 'async_futures'
  #
  # # ProcessExecutor loaded
  # require 'async_futures/process_executor'
  # ```
  #
  # This is because it depends on the `'base64'` gem.
  # This gem was bundled in Ruby 3.3 and prior,
  # but was unbundled in 3.4 and later
  # (even though it is still the Ruby core team that maintains this gem).
  # One goal of `AsyncFutures` is to have no hard dependencies
  # on code outside the standard library.
  # Because this Executor does have a hard gem dependency,
  # it is not loaded by default.
  #
  # If you want to use this Executor in Ruby 3.4 or later,
  # you will need to install the `'base64'` gem as well.
  #
  # For `ProcessExecutor` the tasks are never run immediately upon submission.
  # They are placed into a work queue
  # to be picked up later.
  #
  # Process workers are not reused for work
  # like Threads and Ractors are.
  # Each task gets a freshly forked process.
  # This is because marshalling anonymous blocks is not trivial in Ruby;
  # it is simpler to just fork after the block closure has been defined.
  # Use `ThreadExecutor` or `RactorExecutor`
  # for `Executor` implementations that support worker reuse.
  #
  # Consequently, this executor is only really useful for expensive calculations
  # where the startup time for a process
  # is dwarfed by the time needed for the actual work.
  # Although modern machines can fork a process thousands of times per second,
  # this is very, very slow when machines can do billions of operations per second.
  #
  # If `RactorExecutor` is available on your Ruby engine/version
  # it is probably a better choice for parallel work.
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
    # to optionally add a prefix to generated worker names.
    #
    # The parameter `daemonize_workers`,
    # if set to `true`,
    # causes workers to reparent under the init process
    # and allow it to reap them.
    # If set to `false`,
    # this will cause the Executor instance to use `Process.detach`
    # on the PID of each spawned worker,
    # which will create an extra Ruby thread to reap the PID of each worker.
    # It defaults to `false`.
    def initialize(
      max_workers: nil,
      worker_name_prefix: nil,
      daemonize_workers: false
    )
      @max_workers = (max_workers || [32, Etc.nprocessors + 4].min).to_i
      @worker_name_prefix = worker_name_prefix
      @daemonize_workers = daemonize_workers

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

        synchronize { @futures[future.object_id] = future } # rubocop:disable Lint/HashCompareByIdentity
        @tasks.push(task_ary)
        maybe_spawn_task_feeder
        maybe_spawn_result_feeder
      rescue ClosedQueueError
        @futures.delete(future.object_id)
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

        synchronize { wait_until { all_work_complete? } } if wait
      end
    end

    private

    # If the Executor is shutdown *AND* all remaining work as been completed.
    #
    # Must be called within a `synchronize` block.
    def all_work_complete?
      @pool.empty? && @tasks.closed? && @tasks.empty? && @futures.empty?
    end

    # The smallest positive float value,
    # and thus the smallest possible timeout value.
    SMALLEST_TIMEOUT = 0.0.next_float

    private_constant :SMALLEST_TIMEOUT

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

          unless future.set_running_or_notify_cancel
            synchronize { @futures.delete(future.object_id) }
            next
          end

          future_object_id = future.object_id

          read_pipe, write_pipe, worker_name = synchronize do
            wait_until { @pool.size < @max_workers }

            read_pipe, write_pipe = IO.pipe
            @pool.add(read_pipe)
            [read_pipe, write_pipe, new_worker_name]
          end

          pid = Process.fork do
            # :nocov:
            Process.daemon(true, true) if @daemonize_workers
            # :nocov:

            read_pipe.close
            AsyncFutures.worker_name = worker_name
            result = block.call(*args, **kwargs)
            marshalled_result = Marshal.dump(result)
            b64_enc = Base64.strict_encode64(marshalled_result)
            json_result = JSON.dump([future_object_id, :result, b64_enc])
          rescue Exception => e # rubocop:disable Lint/RescueException
            marshalled_exc = Marshal.dump(e)
            b64_enc = Base64.strict_encode64(marshalled_exc)
            json_exc = JSON.dump([future_object_id, :exception, b64_enc])
            write_pipe.write(json_exc)
          else
            write_pipe.write(json_result)
          ensure
            write_pipe.close
          end

          Process.detach(pid) unless @daemonize_workers
          write_pipe.close
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

    def spawn_result_feeder # rubocop:disable Metrics/AbcSize
      @result_feeder = Thread.new("result_feeder_#{object_id}") do |feeder_name|
        Thread.current.name = feeder_name

        loop do
          break_loop, results_pipes = synchronize do
            wait_until { all_work_complete? || !@pool.empty? }

            [all_work_complete?, @pool.dup]
          end

          break if break_loop

          next_pipe = results_pipes.lazy.filter { |p| p.wait_readable(SMALLEST_TIMEOUT) }.first

          next if next_pipe.nil?

          synchronize { @pool.delete(next_pipe) }

          msg_raw = begin
            next_pipe.read
          ensure
            next_pipe.close
          end

          msg = JSON.parse(msg_raw)
          future_id, type, value = msg
          b64_dec = Base64.strict_decode64(value)
          unmarshalled_value = Marshal.load(b64_dec) # rubocop:disable Security/MarshalLoad
          future = synchronize { @futures.delete(future_id) { raise "future_id not found #{future_id}" } }

          future.set_exception(unmarshalled_value) if type.to_sym.equal? :exception
          future.set_result(unmarshalled_value) if type.to_sym.equal? :result
        end
      ensure
        synchronize do
          @result_feeder = nil
        end
      end
    end

    # def log_debug(&)
    #   AsyncFutures.logger&.debug(&)
    # end
  end
end
