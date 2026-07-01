# frozen_string_literal: true

# The Ractor API was different before version 4.x of Ruby.
if /^3\./ === RUBY_VERSION
  raise LoadError.new("'async_futures/ractor_executor' is not supported in Ruby version '#{RUBY_VERSION}'")
end

require_relative 'executor'
require_relative 'ractor_executor/spawn_worker'

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
    def initialize( # rubocop:disable Metrics/AbcSize,Metrics/ParameterLists
      max_workers: nil,
      worker_name_prefix: nil,
      move_result: false,
      move_args: false,
      make_args_shareable: false,
      copy_args: false
    )
      if copy_args && !make_args_shareable
        raise ArgumentError.new('`copy_args` cannot be true unless `make_args_shareable` is also true')
      end

      AsyncFutures.logger&.debug('RactorExecutor') { "starting RactorExecutor #{object_id}" }

      @max_workers = (max_workers || [32, Etc.nprocessors + 4].min).to_i
      @worker_name_prefix = worker_name_prefix

      # This value is passed into worker Ractors.
      # If the caller passed something not shareable,
      # it would error when we spawn workers later.
      # Boolean values are always safely shareable
      # and since we only care about the truthiness of this value
      # double negation makes sense here.
      @move_result = !!move_result # rubocop:disable Style/DoubleNegation

      @move_args = move_args
      @make_args_shareable = make_args_shareable
      @copy_args = copy_args
      @mutex = Thread::Mutex.new
      @condition = Thread::ConditionVariable.new
      @tasks = Thread::Queue.new
      @available_workers = Thread::Queue.new

      # All private variables after this point
      # require synchronization to safely interact with.
      @work_ports = {}
      @futures = {}

      # When Fibers are eventually supported,
      # a worker can/will have more than one future associated with it.
      @worker_futures = Hash.new { |hash, key| hash[key] = Set.new }

      @results_ports = {}
      @pool = Set.new
      @worker_count = 0

      @task_feeder = nil
      @result_feeder = nil

      # The inter-thread communication between these is necessary for shutdown,
      # so even if nothing is submitted, we still need these to exist for now.
      maybe_spawn_task_feeder
      maybe_spawn_result_feeder

      at_exit { shutdown(wait: false) }
      AsyncFutures.logger&.debug('RactorExecutor') { "started RactorExecutor #{object_id}" }
    end

    # Asynchronously submit a task for execution.
    #
    # See `AsyncFutures::Executor.submit` method for full documentation.
    def submit(*args, **kwargs, &block) # rubocop:disable Metrics/AbcSize
      AsyncFutures.logger&.debug('RactorExecutor') { "Submitting to RactorExecutor #{object_id}" }
      raise ArgumentError.new('No block given') unless block

      Future.new.tap do |future|
        # Attempt to make everything shareable upon submit
        # so that if making shareable would raise an exception
        # the caller can know immediately
        # that a given value won't work.
        # Otherwise the errors could easily get swallowed in a background thread
        # and the caller would never know.
        sh_block = Ractor.shareable_proc(&block)
        sh_args = @make_args_shareable ? Ractor.make_shareable(args, copy: @copy_args) : args
        sh_kwargs = @make_args_shareable ? Ractor.make_shareable(kwargs, copy: @copy_args) : kwargs
        ractor_task = [future, sh_block, sh_args, sh_kwargs]

        @tasks.push(ractor_task)
        maybe_spawn_worker
        maybe_spawn_task_feeder
        maybe_spawn_result_feeder

        AsyncFutures.logger&.debug('RactorExecutor') { "Submitted to RactorExecutor #{object_id}" }
      rescue ClosedQueueError
        raise 'RactorExecutor instance is shutdown'
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

        synchronize { wait_until { @available_workers.closed? && @available_workers.empty? } } if wait
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

    def wait_while
      @condition.wait(@mutex) while yield
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

    def spawn_task_feeder # rubocop:disable Metrics/AbcSize,Metrics/PerceivedComplexity,Metrics/CyclomaticComplexity
      @task_feeder = Thread.new("task_feeder_#{object_id}") do |feeder_name|
        Thread.current.name = feeder_name

        AsyncFutures.logger&.debug('RactorExecutor') { "started feeder #{Thread.current.name}" }

        while (task = @tasks.pop)
          future, block, args, kwargs = task

          AsyncFutures.logger&.debug('RactorExecutor') { "Attempting task #{task}" }

          next unless future.set_running_or_notify_cancel

          AsyncFutures.logger&.debug('RactorExecutor') { "set running task #{task}" }

          if (next_worker = @available_workers.pop)
            AsyncFutures.logger&.debug('RactorExecutor') { "retrieved next worker #{next_worker}" }
            begin
              synchronize do
                AsyncFutures.logger&.debug('RactorExecutor') { "saving future #{future.object_id}: #{future}" }
                @futures[future.object_id] = future # rubocop:disable Lint/HashCompareByIdentity
                @worker_futures[next_worker].add(future)
              end

              # `block` was already made shareable
              # in the submitting thread.
              # `args` and `kwargs` _may_ have been made shareable already.
              # `object_id` is an `Integer`
              # and thus is inherently immutable and shareable.
              ractor_task = [future.object_id, block, args, kwargs].freeze

              AsyncFutures.logger&.debug('RactorExecutor') { "sending task to next worker #{next_worker}" }
              next_worker.send(ractor_task, move: @move_args)
            rescue Exception => e # rubocop:disable Lint/RescueException
              AsyncFutures.logger&.debug('RactorExecutor') { "sending task to ractor worker failed #{e}" }
              synchronize do
                @futures.delete(future.object_id)
                @worker_futures[next_worker].delete(future)
              end
              future.set_exception(e)
            end
          else
            # FIXME: I'm not even certain it is possible to trigger this branch.
            future.set_exception(RactorError.new('Worker queue closed'))
          end
        end

        AsyncFutures.logger&.debug('RactorExecutor') { 'sending shutdown signal to workers' }
        # once the task queue closes
        # that means the executor is shutdown.
        # We need to shutdown all workers until
        # the worker queue gets closed by the `@results_feeder`.
        while (next_worker = @available_workers.pop)
          AsyncFutures.logger&.debug('RactorExecutor') { "sending shutdown signal to worker #{next_worker}" }
          next_worker.send(:shutdown)
        end
        AsyncFutures.logger&.debug('RactorExecutor') { 'shutdown signal sent to all workers' }
      ensure
        synchronize do
          AsyncFutures.logger&.debug('RactorExecutor') { 'Clearing @task_feeder reference' }
          if @task_feeder.equal? Thread.current
            @task_feeder = nil
            AsyncFutures.logger&.debug('RactorExecutor') { '@task_feeder reference set to nil' }
          else
            AsyncFutures.logger&.debug('RactorExecutor') { '@task_feeder reference did not point to current thread' }
          end
        end
      end
    end

    def maybe_spawn_result_feeder
      synchronize { spawn_result_feeder unless @result_feeder }
    end

    def spawn_result_feeder # rubocop:disable Metrics/AbcSize,Metrics/CyclomaticComplexity,Metrics/PerceivedComplexity
      @result_feeder = Thread.new("result_feeder_#{object_id}") do |feeder_name|
        Thread.current.name = feeder_name

        AsyncFutures.logger&.debug('RactorExecutor') { "started feeder #{Thread.current.name}" }
        loop do
          break_loop, work_ports_keys = synchronize do
            wait_until { !@work_ports.empty? || (@pool.empty? && @tasks.closed? && @tasks.empty?) }

            [@work_ports.empty? && @pool.empty? && @tasks.closed? && @tasks.empty?, @work_ports.keys]
          end

          break if break_loop

          AsyncFutures.logger&.debug('RactorExecutor') do
            "waiting until a ractor worker has results to feed #{work_ports_keys}"
          end
          port, msg = Ractor.select(*work_ports_keys)
          AsyncFutures.logger&.debug('RactorExecutor') { "Received a message from worker ractor #{msg}" }

          case msg
          when :exited
            synchronize do
              @work_ports[port].tap do |worker|
                AsyncFutures.logger&.debug('RactorExecutor') { "Cleaning up ractor that exited normally #{worker}" }
                @pool.delete worker
                @work_ports.delete(port)
                port.close
              end
            end
          when :aborted
            old_worker = synchronize do
              @work_ports[port].tap do |old_worker|
                AsyncFutures.logger&.debug('RactorExecutor') do
                  "Cleaning up ractor that exited abnormally #{old_worker}"
                end
                @pool.delete old_worker
                @work_ports.delete(port)
                port.close
              end
            end

            AsyncFutures.logger&.error('RactorExecutor') { "Ractor failed unexpectedly: #{old_worker}" }

            AsyncFutures.logger&.debug('RactorExecutor') { "maybe spawn new worker to replace #{old_worker}" }
            maybe_spawn_worker

            AsyncFutures.logger&.debug('RactorExecutor') do
              "attempting to cancel futures associated with old worker #{old_worker}"
            end
            futures = synchronize { @worker_futures.delete(old_worker) || Set.new }
            begin
              old_worker.join
            rescue Exception => e # rubocop:disable Lint/RescueException
              AsyncFutures.logger&.debug('RactorExecutor') { "old worker raised exception #{e}" }
              futures.each { |f| f.set_exception(e) }
            end
          when Array
            future_id, type, value = msg

            AsyncFutures.logger&.debug('RactorExecutor') { "Regular message received from worker  #{msg}" }

            future = synchronize { @futures[future_id] }

            case type
            when :exception
              AsyncFutures.logger&.debug('RactorExecutor') { "message exception #{value}" }
              future.set_exception(value)
            when :result
              AsyncFutures.logger&.debug('RactorExecutor') { "message result #{value}" }
              future.set_result(value)
            else
              exc_msg = "Unknown result type #{type}"
              exception = RactorExecutor.new(exc_msg)
              future.set_exception(exception)
              AsyncFutures.logger&.error('RactorExecutor') { exc_msg }
            end
            synchronize { @available_workers.push @work_ports[port] }
          else
            exc_msg = "Unknown result symbol #{msg}"
            exception = RactorExecutor.new(exc_msg)
            future.set_exception(exception)
            AsyncFutures.logger&.error('RactorExecutor') { exc_msg }
            raise RactorError.new("Unknown message received: #{task}")
          end
        end

        AsyncFutures.logger&.debug('RactorExecutor') do
          "closing the available_workers queue. It should be not closed? #{@available_workers.closed?}"
        end
        # We synchronize here so that we broadcast the condition afterward.
        synchronize { @available_workers.close }
      end
    end

    # Only spawn a worker if one is needed.
    def maybe_spawn_worker
      # synchronize when interacting directly with @pool
      synchronize { spawn_worker if !@tasks.empty? && @pool.size < @max_workers }
    end
  end
end
