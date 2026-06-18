# frozen_string_literal: true

require_relative 'logger'
require_relative 'error'

require 'timeout'

module AsyncFutures
  # Class for async execution results.
  #
  # Heavily inspired by Python's `concurrent.futures.Future` class.
  class Future # rubocop:disable Metrics/ClassLength
    class << self
      # Returns an Enumerator over the Future instances
      # (possibly created by different Executor instances)
      # given by `futures` that yields futures as they complete
      # (finished or cancelled futures).
      # Any futures given by `futures` that are duplicated will be returned once.
      # Any futures that completed before `as_completed()` is called will be yielded first.
      # The returned Enumerator raises a `Timeout::Error` if `each` or `next()` is called
      # and the result isn’t available after `timeout` seconds from the original call to `as_completed()`.
      # `timeout` can be an int or float.
      # If `timeout` is not specified or `nil`,
      # there is no limit to the wait time.
      def as_completed(futures, timeout = nil) # rubocop:disable Metrics/AbcSize,Metrics/CyclomaticComplexity,Metrics/PerceivedComplexity
        clock_timeout = Time.now.to_f + timeout if timeout
        mtx = Thread::Mutex.new
        queue = Thread::Queue.new

        fs_ary = futures.to_a
        fs_set = fs_ary.to_set
        fs_sze = fs_set.size
        fs_cnt = fs_sze
        fs_set.clear

        fs_ary.each do |future|
          future.add_done_callback do |done_future|
            mtx.synchronize do
              unless fs_set.include? done_future
                fs_set.add done_future
                queue << done_future
                fs_cnt -= 1

                queue.close if fs_cnt.zero?
              end
            end
          end
        end

        Enumerator.new(fs_sze) do |yielder|
          if timeout
            local_timeout = clock_timeout - Time.now.to_f
            raise Timeout::Error unless local_timeout.positive?

            Timeout.timeout(local_timeout) do
              while (done_future = queue.pop)
                yielder << done_future
              end
            end
          else
            while (done_future = queue.pop)
              yielder << done_future
            end
          end
        end
      end
    end

    # Create a new Future instance in a pending state.
    # Should generally only be called by Executor implementations.
    def initialize
      @mutex = Thread::Mutex.new
      @condition = Thread::ConditionVariable.new
      @state = PENDING
      @result = nil
      @exception = nil
      @waiters = []
      @done_callbacks = []
      @thread = nil
      @fiber = nil
    end

    # The Fiber that owns the work for this Future.
    # Used to detect deadlocks.
    # Not for direct use.
    # Should only be used by Future and Executor implementations.
    def fiber
      @mutex.synchronize { @fiber }
    end

    # Set fiber attribute.
    # Should only be used by Future and Executor implementations.
    def fiber=(value)
      @mutex.synchronize { @fiber = value }
    end

    # The Thread that owns the work for this Future.
    # Used to detect deadlocks.
    # Not for direct use.
    # Should only be used by Future and Executor implementations.
    def thread
      @mutex.synchronize { @thread }
    end

    # Set thread attribute.
    # Should only be used by Future and Executor implementations.
    def thread=(value)
      @mutex.synchronize { @thread = value }
    end

    # Attempt to cancel the call. If the call is currently being executed or
    # finished running and cannot be cancelled then the method will return
    # `False`, otherwise the call will be cancelled and the method will return
    # `True`.
    def cancel # rubocop:disable Naming/PredicateMethod
      @mutex.synchronize do
        return true if lockless_cancelled?
        return false if lockless_running? || lockless_finished?

        # The only other state left is PENDING, so we can safely cancel.
        @state = CANCELLED
        @condition.broadcast
      end

      invoke_callbacks
      true
    end

    # Return `True` if the call has not yet started.
    #
    # Not present on Python `concurrent.futures.Future` class.
    def pending?
      @mutex.synchronize { lockless_pending? }
    end

    # Return `True` if the call finished running and was not cancelled.
    #
    # Not present on Python `concurrent.futures.Future` class.
    def finished?
      @mutex.synchronize { lockless_finished? }
    end

    # Return `True` if the call was successfully cancelled.
    def cancelled?
      @mutex.synchronize { lockless_cancelled? }
    end

    # Return `True` if the call is currently being executed and cannot be cancelled.
    def running?
      @mutex.synchronize { lockless_running? }
    end

    # Return `True` if the call was successfully cancelled or finished running.
    def done?
      @mutex.synchronize { lockless_done? }
    end

    # Return the value returned by the call. If the call hasn't yet completed
    # then this method will wait up to `timeout` seconds. If the call
    # hasn't completed in `timeout` seconds, then a `Timeout::Error` will
    # be raised. `timeout` can be an int or float. If  `timeout` is not
    # specified or `nil`, there is no limit to the wait time.
    #
    # If the future is cancelled before completing then `CancelledError` will
    # be raised.
    #
    # If the call raised an exception, this method will raise the same
    # exception.
    def result(timeout = nil)
      private_join(timeout) do
        raise CancelledError if lockless_cancelled?
        raise @exception if @exception

        @result
      end
    end

    # Return the exception raised by the call. If the call hasn't yet completed
    # then this method will wait up to `timeout` seconds. If the call
    # hasn't completed in `timeout` seconds, then a `Timeout::Error` will
    # be raised. `timeout` can be an int or float. If `timeout` is not
    # specified or `nil`, there is no limit to the wait time.
    #
    # If the future is cancelled before completing then `CancelledError` will
    # be raised.
    #
    # If the call completed without raising, `nil` is returned.
    def exception(timeout = nil)
      private_join(timeout) do
        raise CancelledError if lockless_cancelled?

        @exception
      end
    end

    # Wait for future to be `done?`
    # (through regular completion, exception, or cancellation),
    # then return `self`.
    # If the call hasn't yet completed
    # then this method will wait up to `timeout` seconds.
    # If the call hasn't completed in `timeout` seconds,
    # then `nil` will be returned.
    # `timeout` can be an int or float.
    # If `timeout` is not specified or `nil`,
    # there is no limit to the wait time.
    #
    # Calling `join` with a `timeout` value of zero
    # will return immediately.
    # This is effectively equivalent to calling `done?`.
    #
    # Not present on Python's `concurrent.futures.Future` class.
    def join(timeout = nil)
      return (done? && self) || nil if timeout&.zero?

      private_join(timeout) do
        self
      end
    rescue Timeout::Error
      nil
    end

    # Attaches a block that will be called when the future finishes.
    #
    # The block will be called with this future as its only argument
    # when the future completes or is cancelled.
    # The block will always be called by a Thread in the same Ractor
    # in which it was added.
    # If the future has already completed
    # or been cancelled then the block will be called immediately.
    # These blocks are called in the order that they were added.
    def add_done_callback(&block)
      raise ArgumentError.new('No block given') unless block

      @mutex.synchronize do
        unless lockless_done?
          @done_callbacks.append(block)
          return
        end
      end

      # If we reached here, the future already ended, just call the block immediately.
      begin
        block.call(self)
      rescue Exception # rubocop:disable Lint/RescueException
        logger&.error { "Exception calling callback for #{self}" }
      end
    end

    def set_running_or_notify_cancel
      @mutex.synchronize do
        case @state
        when CANCELLED
          @state = CANCELLED_AND_NOTIFIED
          @condition.broadcast
          return false
        when PENDING
          @state = RUNNING
          @condition.broadcast
          return true
        else
          logger&.unknown { "Future #{self} in unexpected state #{@state}" }
          raise InvalidStateError.new('Future in unexpected state')
        end
      end
    end

    def set_result(result) # rubocop:disable Naming/AccessorMethodName
      @mutex.synchronize do
        raise InvalidStateError.new("#{@state}: #{self}") if lockless_done?

        @result = result
        @state = FINISHED
        @condition.broadcast
      end
      invoke_callbacks
    end

    def set_exception(exception) # rubocop:disable Naming/AccessorMethodName
      @mutex.synchronize do
        raise InvalidStateError.new("#{@state}: #{self}") if lockless_done?
        raise ArgumentError.new("Not an Exception: #{exception.inspect}") unless exception.is_a?(Exception)

        @exception = exception
        @state = FINISHED
        @condition.broadcast
      end
      invoke_callbacks
    end

    FIRST_COMPLETED = :FIRST_COMPLETED
    FIRST_EXCEPTION = :FIRST_EXCEPTION
    ALL_COMPLETED = :ALL_COMPLETED
    AS_COMPLETED = :_AS_COMPLETED

    # Possible future states (for internal use by the futures package).

    # Not yet started.
    PENDING = :PENDING

    # Has a worker doing work to complete it.
    RUNNING = :RUNNING

    # The future was cancelled.
    CANCELLED = :CANCELLED

    # `_Waiter.add_cancelled()` was called by a worker.
    # FIXME: I'm 99% certain that the waiter and notify stuff has to do with
    # Python's implementation of Process/Pipe based parallelism. This will
    # probably still be needed for Ractors and Ports, but I don't understand it
    # well enough to add it yet. It will need to wait for another day.
    CANCELLED_AND_NOTIFIED = :CANCELLED_AND_NOTIFIED

    # Finished running, via either success or exception.
    FINISHED = :FINISHED

    private

    def private_join(timeout, &block)
      Timeout.timeout(timeout) do
        @mutex.synchronize do
          unless lockless_done?
            raise DeadlockError.new(self) if Fiber.blocking? && Thread.current.equal?(@thread)
            raise DeadlockError.new(self) if Fiber.current.equal?(@fiber)
          end

          @condition.wait(@mutex) until lockless_done?
          block.call
        end
      end
    end

    def invoke_callbacks
      @done_callbacks.each do |callback|
        callback.call(self)
      rescue Exception # rubocop:disable Lint/RescueException
        logger&.error { "Exception calling callback for #{self}" }
      end
    end

    def logger
      AsyncFutures.logger
    end

    # Only safe to use these methods within the synchronized mutex.
    # Thus why they are private.
    def lockless_cancelled?
      [CANCELLED, CANCELLED_AND_NOTIFIED].include? @state
    end

    def lockless_finished?
      @state.equal? FINISHED
    end

    def lockless_pending?
      @state.equal? PENDING
    end

    def lockless_running?
      @state.equal? RUNNING
    end

    def lockless_done?
      [CANCELLED, CANCELLED_AND_NOTIFIED, FINISHED].include? @state
    end
  end
end
