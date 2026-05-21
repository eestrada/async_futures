# frozen_string_literal: true

require_relative 'executor'

require 'etc'
require 'set' # rubocop:disable Lint/RedundantRequireStatement

module AsyncFutures
  # `Executor` implementation based on `Thread` primitives
  # that uses a pool of up to `max_workers` to execute calls concurrently.
  class ThreadExecutor
    include Executor
    include MonitorMixin

    # Create a new `ThreadExecutor`
    # that uses a pool of up to `max_workers`
    # to execute calls concurrently.
    #
    # The parameter `thread_name_prefix` can be used
    # to optionally add a prefix to generated `Thread` names.
    def initialize(max_workers: nil, thread_name_prefix: '')
      @max_workers = (max_workers || [32, Etc.nprocessors + 4].min).to_i
      @thread_name_prefix = thread_name_prefix.to_s
      @tasks = Thread::Queue.new
      @pool = Set.new
      @cond = new_cond

      at_exit { shutdown(wait: false) }
    end

    # Asynchronously submit a task for execution.
    # For `ThreadExecutor` the tasks are always executed concurrently.
    def submit(*args, **kwargs, &block) # rubocop:disable Metrics/AbcSize,Metrics/CyclomaticComplexity,Metrics/PerceivedComplexity
      raise ArgumentError.new('No block given') unless block

      synchronize do
        raise 'ThreadExecutor instance is shutdown' if @tasks.closed?

        Future.new.tap do |future|
          @tasks.push([future, block, args, kwargs])

          # synchronize when interacting directly with @pool
          if @pool.empty? || (@tasks.size > 1 && @pool.size < @max_workers)
            @pool << Thread.new(
              @thread_name_prefix,
              @tasks,
              @pool,
              method(:synchronize)
            ) do |thread_name_prefix, tasks, pool, sync_proc|
              Thread.current.name = "#{thread_name_prefix}_#{Thread.current.name}" unless thread_name_prefix.empty?

              while (task = tasks.pop)
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
              sync_proc.call do
                pool.delete Thread.current
              end
            end
          end
        end
      end
    end

    alias submit_concurrent submit

    def shutdown(wait: true, cancel_futures: false, &block)
      block&.call(self)
    ensure
      synchronize do
        @tasks.close

        if cancel_futures
          while (task = @tasks.pop)
            future = task[0]
            future.cancel
          end
        end

        if wait
          @pool.dup.each do |thread|
            @cond.wait_until { thread.join(0) }
            @pool.delete thread
            @cond.broadcast
          end
        end
      end
    end
  end
end
