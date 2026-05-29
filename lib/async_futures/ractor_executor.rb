# frozen_string_literal: true

# The Ractor API was different before version 4.x of Ruby.
if /^3\./ === RUBY_VERSION
  raise LoadError.new("'async_futures/ractor_executor' is not supported in Ruby version '#{RUBY_VERSION}'")
end

require_relative 'executor'

require 'etc'
require 'set' # rubocop:disable Lint/RedundantRequireStatement

module AsyncFutures
  # `Executor` implementation based on `Thread` primitives
  # that uses a pool of up to `max_workers` to execute calls concurrently.
  #
  # `RactorExecutor` specific submission considerations:
  #
  # For `RactorExecutor` the tasks are never run immediately upon submission.
  # They are placed into a work queue
  # to be picked up later by worker threads.
  #
  # This does _not_ guarantee
  # that any particular task will be run concurrently
  # with any other particular task;
  # that is dependent on how many worker threads and tasks there are
  # at any given point in time.
  class RactorExecutor # rubocop:disable Metrics/ClassLength
    include Executor

    # Create a new `RactorExecutor`.
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
    # If the `move_result` keyword argument is `true`,
    # results from worker ractors will be moved instead of copied.
    # Moving is faster, but less safe
    # if the worker ractor keeps the values around for some reason.
    def initialize(max_workers: nil, worker_name_prefix: nil, move_result: false) # rubocop:disable Metrics/AbcSize
      @max_workers = (max_workers || [32, Etc.nprocessors + 4].min).to_i
      @worker_name_prefix = worker_name_prefix
      @move_result = !!move_result # rubocop:disable Style/DoubleNegation
      @mutex = Thread::Mutex.new
      @tasks = Thread::Queue.new
      @available_workers = Thread::Queue.new
      @work_ports = @max_workers.times.to_h { [Ractor::Port.new, nil] }
      @futures = {}
      @results_ports = {}
      @pool = Set.new
      @worker_count = 0
      @task_feeder = nil
      @result_feeder = nil

      at_exit { shutdown(wait: false) }
    end

    # Asynchronously submit a task for execution.
    #
    # See `AsyncFutures::Executor.submit` method for full documentation.
    def submit(*args, **kwargs, &block)
      raise ArgumentError.new('No block given') unless block
      raise 'RactorExecutor instance is shutdown' if @tasks.closed?

      Future.new.tap do |future|
        @tasks.push([future, block, args, kwargs])
        maybe_spawn_task_feeder
        maybe_spawn_result_feeder
        maybe_spawn_worker
      end
    end

    alias submit_concurrent submit

    public :map

    # Shutdown `RactorExecutor` instance.
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

    def new_worker_name
      synchronize do
        if @worker_name_prefix
          "#{@worker_name_prefix}_#{@worker_count += 1}"
        else
          "#{self.class.name}_#{object_id}_worker_#{@worker_count += 1}"
        end
      end
    end

    def maybe_spawn_task_feeder
      synchronize { spawn_task_feeder unless @task_feeder }
    end

    def spawn_task_feeder # rubocop:disable Metrics/AbcSize
      @task_feeder = Thread.new("task_feeder_#{object_id}") do |feeder_name|
        feeder.name = feeder_name

        while (task = @tasks.pop)
          future, block, args, kwargs = task

          next unless future.set_running_or_notify_cancel

          if (next_ractor = @available_workers.pop)
            begin
              @futures[future.object_id] = future # rubocop:disable Lint/HashCompareByIdentity

              ractor_task = [
                future.object_id,
                Ractor.shareable_proc(block),
                Ractor.make_shareable(args, copy: true),
                Ractor.make_shareable(kwargs, copy: true),
              ]

              next_ractor.send(ractor_task, move: false)
            rescue Exception => e # rubocop:disable Lint/RescueException
              @futures.delete(future.object_id)
              future.set_exception(e)
            end
          else
            future.set_exception(RactorError.new('Worker queue closed'))
          end
        end

        while (next_ractor = @available_workers.pop)
          next_ractor.send(:shutdown)
        end
      end
    end

    def maybe_spawn_result_feeder
      synchronize { spawn_result_feeder unless @result_feeder }
    end

    def spawn_result_feeder # rubocop:disable Metrics/AbcSize,Metrics/CyclomaticComplexity
      @result_feeder = Thread.new("result_feeder_#{object_id}") do |feeder_name|
        feeder.name = feeder_name

        loop do
          port, msg = Ractor.select(*@work_ports.keys)

          case msg
          when Symbol
            case msg
            when :exited
              # remove ractor from mappings
              ractor = @work_ports[port]
              @work_ports[port] = nil
              @pool.delete ractor
            when :aborted # rubocop:disable Lint/DuplicateBranch
              # remove ractor from mappings
              # And report error. check Ractor.value to get final exception value.
              ractor = @work_ports[port]
              @work_ports[port] = nil
              @pool.delete ractor

              # TODO: if there is an in flight future
              # associated with this ractor
              # that didn't get addressed,
              # then we need to finish the future exceptionally.
            else
              exc_msg = "Unknown result symbol #{msg}"
              exception = RactorExecutor.new(exc_msg)
              future.set_exception(exception)
              AsyncFutures.logger&.error('RactorExecutor') { exc_msg }
            end
          when Array
            future_id, type, value = msg

            future = @futures[future_id]

            case type
            when :exception
              future.set_exception(value)
            when :result
              future.set_result(value)
            else
              exc_msg = "Unknown result type #{type}"
              exception = RactorExecutor.new(exc_msg)
              future.set_exception(exception)
              AsyncFutures.logger&.error('RactorExecutor') { exc_msg }
            end
          else
            raise RactorError.new("Unknown message received: #{task}")
          end
        end
      end
    end

    # Only spawn a worker if one is needed.
    def maybe_spawn_worker
      # synchronize when interacting directly with @pool
      synchronize { spawn_worker if !@tasks.empty? && @pool.size < @max_workers }
    end

    # Always spawn a worker
    def spawn_worker # rubocop:disable Metrics/AbcSize
      available_port = @work_ports.find { |_key, value| value.nil? }.first
      worker = Ractor.new(available_port, @move_result, name: new_worker_name) do |results_port, move_result|
        loop do
          case (task = Ractor.receive)
          when :shutdown
            break
          when Array
            future_id, block, args, kwargs = task
          else
            raise RactorError.new("Unknown message received: #{task}")
          end

          begin
            result = block.call(*args, **kwargs)
          rescue Exception => e # rubocop:disable Lint/RescueException
            results_port.send([future_id, :exception, e], move: move_result)
          else
            results_port.send([future_id, :result, result], move: move_result)
          end
        end
      end

      @work_ports[available_port] = worker
      worker.monitor available_port
      @pool.add worker
      @available_workers.push worker
    end
  end
end
