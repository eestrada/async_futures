# frozen_string_literal: true

require_relative 'executor'

require 'etc'

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
      @pool = []
    end

    # Asynchronously submit a task for execution.
    # For `ThreadExecutor` the tasks are always executed concurrently.
    def submit(*args, **kwargs, &block) # rubocop:disable Metrics/AbcSize,Metrics/CyclomaticComplexity,Metrics/PerceivedComplexity
      raise ArgumentError.new('No block given') unless block

      Future.new.tap do |future|
        raise 'ThreadExecutor instance is shutdown' if @tasks.closed?

        @tasks.push([future, block, args, kwargs])

        # synchronize when interacting directly with @pool
        synchronize do
          if @pool.empty? || (@tasks.size > 1 && @pool.size < @max_workers)
            @pool << Thread.new(@thread_name_prefix, @tasks) do |thread_name_prefix, tasks|
              Thread.current.name = "#{thread_name_prefix}_#{Thread.current.name}" unless thread_name_prefix.empty?

              while (task = tasks.pop)
                future, block, args, kwargs = task

                next unless future.set_running_or_notify_cancel

                begin
                  result = block.call(*args, **kwargs)
                rescue Exception => e # rubocop:disable Lint/RescueException
                  future.set_exception(e)
                else
                  future.set_result(result)
                end
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
      @tasks.close

      if cancel_futures
        while (task = @tasks.pop)
          future = task[0]
          future.cancel
        end
      end

      # TODO: Is there a race condition?
      # Do I need to synchronize this twice?
      # It's late and I can't think straight about race conditions clearly.
      synchronize { @pool.each(&:join) } if wait
    end
  end
end
