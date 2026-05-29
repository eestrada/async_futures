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
      spawn_task_feeder unless synchronize { @task_feeder }
    end

    def spawn_task_feeder
      feeder = Thread.new("task_feeder_#{object_id}") do |feeder_name|
        feeder.name = feeder_name

        while (task = @tasks.pop)
          future, block, args, kwargs = task

          ractor_task = [
            future.object_id,
            Ractor.shareable_proc(block),
            Ractor.make_shareable(args, copy: true),
            Ractor.make_shareable(kwargs, copy: true),
          ]

          next_ractor = @available_workers.pop

          next_ractor.send(ractor_task)
        end
      end

      synchronize { @task_feeder = feeder }
    end

    def maybe_spawn_result_feeder
      spawn_result_feeder unless synchronize { @result_feeder }
    end

    def spawn_result_feeder
      feeder = Thread.new("result_feeder_#{object_id}") do |feeder_name|
        feeder.name = feeder_name
      end

      synchronize { @result_feeder = feeder }
    end

    # Only spawn a worker if one is needed.
    def maybe_spawn_worker
      # synchronize when interacting directly with @pool
      spawn_worker if !@tasks.empty? && synchronize { @pool.size } < @max_workers
    end

    # Always spawn a worker
    def spawn_worker
      worker = Ractor.new(@results_port, @move_result, name: new_worker_name) do |results_port, move_result|
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
      synchronize { @pool.add worker }
      @available_workers.push worker
    end
  end
end
