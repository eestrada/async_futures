# frozen_string_literal: true

require_relative 'logger'
require_relative 'error'

require 'timeout'

module AsyncFutures
  # Class for async execution results.
  #
  # Heavily inspired by Python's `concurrent.futures.Future` class.
  class Future # rubocop:disable Metrics/ClassLength
    # The `Future.wait` method will return when any future finishes or is cancelled.
    FIRST_COMPLETED = :FIRST_COMPLETED

    # The `Future.wait` method will return when any future finishes by raising an exception.
    # If no future raises an exception then it is equivalent to ALL_COMPLETED.
    FIRST_EXCEPTION = :FIRST_EXCEPTION

    # The `Future.wait` method will return when all futures finish or are cancelled.
    ALL_COMPLETED = :ALL_COMPLETED

    class << self
      # Wait for the `Future` instances
      # (possibly created by different Executor instances)
      # given by `Enumerable` object `futures` to complete.
      # Duplicate futures given to `futures` are removed
      # and will be returned only once.
      #
      # Returns a `Hash` of sets.
      # The first set,
      # keyed to `:done`,
      # contains the futures that completed
      # (finished or cancelled futures)
      # before the wait completed.
      # The second set,
      # keyed to `:not_done`,
      # contains the futures that did not complete
      # (pending or running futures).
      #
      # `timeout` can be used to control the maximum number of seconds to wait before returning.
      # `timeout` can be an int or float.
      # If `timeout` is not specified or `nil`,
      # there is no limit to the wait time.
      #
      # A negative value for `timeout` is allowed
      # and will just return immediately.
      # Already completed futures are still included in this case.
      # In this circumstance,
      # all `return_when` values behave identically.
      #
      # `return_when` indicates when this function should return.
      # See constant descriptions for details.
      def wait(futures, timeout = nil, return_when = ALL_COMPLETED) # rubocop:disable Metrics/AbcSize,Metrics/CyclomaticComplexity,Metrics/PerceivedComplexity
        clock_timeout = Time.now.to_f + timeout if timeout
        mtx = Thread::Mutex.new
        queue = Thread::Queue.new

        fs_ary = futures.to_a.uniq
        fs_cnt = fs_ary.size

        done_set = Set.new
        not_done_set = Set.new(fs_ary)

        return { done: done_set, not_done: not_done_set } if fs_ary.empty?

        case return_when
        when FIRST_COMPLETED
          fs_ary.each do |future|
            future.add_done_callback do |ftr|
              mtx.synchronize do
                queue.push(ftr)
                queue.close
              rescue ClosedQueueError
                # Do nothing
              end
            end
          end
        when FIRST_EXCEPTION
          fs_ary.each do |future|
            future.add_done_callback do |ftr|
              mtx.synchronize do
                queue.push(ftr)
                queue.close if !ftr.cancelled? && ftr.exception
              rescue ClosedQueueError
                # Do nothing
              end
            end
          end
        when ALL_COMPLETED
          fs_ary.each do |future|
            future.add_done_callback do |ftr|
              queue.push(ftr)

              mtx.synchronize do
                fs_cnt -= 1
                queue.close if fs_cnt.zero?
              end
            rescue ClosedQueueError
              # Do nothing
            end
          end
        else
          raise ArgumentError.new("Unknown 'return_when' value '#{return_when}'")
        end

        begin
          cb_timeout = timeout && (clock_timeout - Time.now.to_f)
          raise Timeout::Error unless cb_timeout.nil? || cb_timeout.positive?

          Timeout.timeout(cb_timeout) do
            while (dn_ftr = queue.pop)
              done_set.add(dn_ftr)
            end
          end
        rescue Timeout::Error
          queue.close
          while (dn_ftr = queue.pop)
            done_set.add(dn_ftr)
          end
        end

        done_set.merge(fs_ary.lazy.filter(&:done?))
        { done: done_set, not_done: not_done_set.difference(done_set) }
      end

      # Returns an `Enumerator` over the `Future` instances
      # (possibly created by different `Executor` instances)
      # given by the `Enumerable` object `futures`
      # that yields futures as they complete
      # (finished or cancelled futures).
      #
      # The returned `Enumerator` can only be enumerated over once.
      # Subsequent enumeration attempts will raise `RuntimeError`.
      #
      # Any futures given by `futures` that are duplicated will be returned once.
      #
      # Any futures that completed before `as_completed()` is called will be yielded first.
      #
      # The returned `Enumerator` raises a `Timeout::Error` if `each` or `next()` is called
      # and the result isn’t available after `timeout` seconds
      # from the original call to `as_completed()`.
      # `timeout` can be an int or float.
      # If `timeout` is not specified or `nil`,
      # there is no limit to the wait time.
      def as_completed(futures, timeout = nil) # rubocop:disable Metrics/AbcSize,Metrics/CyclomaticComplexity,Metrics/PerceivedComplexity
        clock_timeout = Time.now.to_f + timeout if timeout
        mtx = Thread::Mutex.new
        queue = Thread::Queue.new
        has_enumerated = false

        fs_ary = futures.to_a.uniq
        fs_sze = fs_ary.size
        fs_cnt = fs_sze

        cb_timeout = timeout && (clock_timeout - Time.now.to_f)
        raise Timeout::Error unless cb_timeout.nil? || cb_timeout.positive?

        Timeout.timeout(cb_timeout) do
          fs_ary.each do |future|
            future.add_done_callback do |done_future|
              queue.push done_future

              mtx.synchronize do
                fs_cnt -= 1
                queue.close if fs_cnt.zero?
              end
            end
          end
        end

        Enumerator.new(fs_sze) do |yielder|
          raise 'Enumerator already consumed' if mtx.synchronize { has_enumerated }

          enum_timeout = timeout && (clock_timeout - Time.now.to_f)
          raise Timeout::Error unless enum_timeout.nil? || enum_timeout.positive?

          Timeout.timeout(enum_timeout) do
            while (done_future = queue.pop)
              yielder.yield done_future
            end
          end
        ensure
          mtx.synchronize { has_enumerated = true }
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
      @done_callbacks = []
      @thread = nil
      @fiber = nil
    end

    # The future can’t be frozen, so this method raises an exception:
    #
    # ```ruby
    # AsyncFutures::Future.new.freeze # Raises TypeError (cannot freeze #<AsyncFutures::Future:0x...>)
    # ```
    def freeze
      raise TypeError.new("cannot freeze #{self}")
    end

    # Convenience method to complete the future
    # with the given block, args, and kwargs.
    #
    # This method will only run the given block
    # if the future is *not* already running, canceled, or completed.
    #
    # It will return `true` if the block was run by this call
    # and `false` if it was *not* run by this call.
    def complete(*args, **kwargs, &block) # rubocop:disable Style/ArgumentsForwarding,Naming/PredicateMethod
      raise ArgumentError.new('No block given') unless block

      begin
        return false unless set_running_or_notify_cancel(set_context: true)
      rescue InvalidStateError
        # RUNNING, CANCELLED_AND_NOTIFIED, or FINISHED states.
        return false
      end

      begin
        result = block.call(*args, **kwargs) # rubocop:disable Style/ArgumentsForwarding
      rescue Exception => e # rubocop:disable Lint/RescueException
        set_exception(e)
      else
        set_result(result)
      end
      true
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

    # This method should only be called by `Executor` implementations
    # before executing the work associated with the `Future`
    # and by unit tests.
    #
    # If the method returns `false` then the `Future` was cancelled,
    # i.e. `Future.cancel` was called and returned `true`.
    # Any threads waiting on the `Future` completing
    # (i.e. through `Future.as_completed()` or `Future.wait()`) will be woken up.
    #
    # If the method returns true
    # then the `Future` was not cancelled
    # and has been put in the running state,
    # i.e. calls to `Future.running?` will return true.
    #
    # This method should only be called once.
    # If it is called more than once,
    # then it will raise an `InvalidStateError` exception.
    # If it is called after `Future.set_result()`
    # or `Future.set_exception()` have been called
    # then it will raise an `InvalidStateError` exception.
    # Thus, this is why it is more of an implementation detail
    # for Executor implementations (or similar).
    def set_running_or_notify_cancel(set_context: false)
      @mutex.synchronize do
        case @state
        when CANCELLED
          @state = CANCELLED_AND_NOTIFIED
          @condition.broadcast
          return false
        when PENDING
          @state = RUNNING
          @condition.broadcast
          if set_context
            @thread = Thread.current
            @fiber = Fiber.current
          end
          return true
        else
          # raised for RUNNING, CANCELLED_AND_NOTIFIED, and FINISHED states.
          raise InvalidStateError.new(self, @state)
        end
      end
    end

    # Sets the result of the work associated with the `Future` to result.
    #
    # This method should only be used by `Executor` implementations and unit tests.
    def set_result(result) # rubocop:disable Naming/AccessorMethodName
      @mutex.synchronize do
        raise InvalidStateError.new(self, @state) if lockless_done?

        @result = result
        @state = FINISHED
        @condition.broadcast
      end
      invoke_callbacks
    end

    # Sets the result of the work associated with the `Future` to the Exception `exception`.
    #
    # This method should only be used by `Executor` implementations and unit tests.
    def set_exception(exception) # rubocop:disable Naming/AccessorMethodName
      @mutex.synchronize do
        raise InvalidStateError.new(self, @state) if lockless_done?
        raise ArgumentError.new("Not an Exception: #{exception.inspect}") unless exception.is_a?(Exception)

        @exception = exception
        @state = FINISHED
        @condition.broadcast
      end
      invoke_callbacks
    end

    private

    # Possible future states (for internal use by the futures package).

    # Not yet started.
    PENDING = :PENDING

    # Has a worker doing work to complete it.
    RUNNING = :RUNNING

    # The future was cancelled.
    CANCELLED = :CANCELLED

    # Future has been cancelled
    # **and** the worker assigned to complete the future has been notified.
    # of the fact that the future has been cancelled.
    # Only set by calling `set_running_or_notify_cancel` on a cancelled future.
    # This prevents future from be set to running or cancelled more than once.
    # Instead it raises an InvalidStateError if this is the state.
    CANCELLED_AND_NOTIFIED = :CANCELLED_AND_NOTIFIED

    # Finished running, via either success or exception.
    FINISHED = :FINISHED

    # Make all internal states private visibility
    private_constant :PENDING, :RUNNING, :CANCELLED, :CANCELLED_AND_NOTIFIED, :FINISHED

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
