# frozen_string_literal: true

require_relative 'flex_futures/version'
require 'timeout'
require 'monitor'

# Library to create futures for Ractors, Threads, and Fibers.
module FlexFutures
  # Configurable logger for the library. Assumes the standard logger interface.
  attr_accessor :logger

  class Error < StandardError; end

  # Class for async execution results.
  class Future # rubocop:disable Metrics/ClassLength
    include MonitorMixin
    include Timeout

    class InvalidStateError < Error
    end

    class CancelledError < Error
    end

    def initialize
      super
      @condition = new_cond
      @state = PENDING
      @result = nil
      @exception = nil
      @waiters = []
      @done_callbacks = []
    end

    # Attempt to cancel the call. If the call is currently being executed or
    # finished running and cannot be cancelled then the method will return
    # `False`, otherwise the call will be cancelled and the method will return
    # `True`.
    def cancel
      synchronize do
        return True if cancelled?
        return False if running? || finished?

        # The only other state left is PENDING, so we can safely cancel.
        @state = CANCELLED
        @condition.broadcast
      end

      invoke_callbacks
      True
    end

    # Return True if the call has not yet started.
    #
    # Not present on Python concurrent.futures.Future class.
    def pending?
      synchronize do
        @state.equal? PENDING
      end
    end

    # Return True if the call finished running and was not cancelled.
    #
    # Not present on Python concurrent.futures.Future class.
    def finished?
      synchronize do
        @state.equal? FINISHED
      end
    end

    # Return True if the call was successfully cancelled.
    def cancelled?
      synchronize do
        [CANCELLED, CANCELLED_AND_NOTIFIED].include? @state
      end
    end

    # Return True if the call is currently being executed and cannot be cancelled.
    def running?
      synchronize do
        @state.equal? RUNNING
      end
    end

    # Return True if the call was successfully cancelled or finished running.
    def done?
      synchronize do
        [CANCELLED, CANCELLED_AND_NOTIFIED, FINISHED].include? @state
      end
    end

    # Return the value returned by the call. If the call hasn’t yet completed
    # then this method will wait up to `timeout_sec` seconds. If the call
    # hasn’t completed in `timeout_sec` seconds, then a `Timeout::Error` will
    # be raised. `timeout_sec` can be an int or float. If  `timeout_sec` is not
    # specified or `nil`, there is no limit to the wait time.
    #
    # If the future is cancelled before completing then `CancelledError` will
    # be raised.
    #
    # If the call raised an exception, this method will raise the same
    # exception.
    def result(timeout_sec = nil)
      timeout(timeout_sec) do
        synchronize do
          @condition.wait_until(&done?)
          raise CancelledError if cancelled?

          raise @exception if @exception

          @result
        end
      end
    end

    # Return the exception raised by the call. If the call hasn’t yet completed
    # then this method will wait up to `timeout_sec` seconds. If the call
    # hasn’t completed in `timeout_sec` seconds, then a `Timeout::Error` will
    # be raised. `timeout_sec` can be an int or float. If `timeout_sec` is not
    # specified or `nil`, there is no limit to the wait time.
    #
    # If the future is cancelled before completing then `CancelledError` will
    # be raised.
    #
    # If the call completed without raising, `nil` is returned.
    def exception(timeout_sec = nil)
      timeout(timeout_sec) do
        synchronize do
          @condition.wait_until(&done?)
          raise CancelledError if cancelled?

          @exception
        end
      end
    end

    # Attaches a block that will be called when the future finishes.
    #
    # Args:
    #     block: A block that will be called with this future as its only
    #         argument when the future completes or is cancelled. The block
    #         will always be called by a thread in the same process in which
    #         it was added. If the future has already completed or been
    #         cancelled then the block will be called immediately. These
    #         callables are called in the order that they were added.
    def add_done_callback(&block)
      raise ArgumentError.new('No block given') unless block

      synchronize do
        unless done?
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
      synchronize do
        case @state
        when CANCELLED
          @state = CANCELLED_AND_NOTIFIED
          return False
        when PENDING
          @state = RUNNING
          return True
        else
          logger&.unknown { "Future #{self} in unexpected state #{@state}" }
          raise InvalidStateError.new('Future in unexpected state')
        end
      end
    end

    def set_result(result) # rubocop:disable Naming/AccessorMethodName
      synchronize do
        raise InvalidStateError.new("#{@state}: #{self}") if done?

        @result = result
        @state = FINISHED
      end
      invoke_callbacks
    end

    def set_exception(exception) # rubocop:disable Naming/AccessorMethodName
      synchronize do
        raise InvalidStateError.new("#{@state}: #{self}") if done?

        @exception = exception
        @state = FINISHED
      end
      invoke_callbacks
    end

    FIRST_COMPLETED = :FIRST_COMPLETED
    FIRST_EXCEPTION = :FIRST_EXCEPTION
    ALL_COMPLETED = :ALL_COMPLETED
    AS_COMPLETED = :_AS_COMPLETED

    # Possible future states (for internal use by the futures package).
    PENDING = :PENDING
    RUNNING = :RUNNING
    # The future was cancelled by the user...
    CANCELLED = :CANCELLED
    # ...and _Waiter.add_cancelled() was called by a worker.

    # FIXME: I'm 99% certain that the waiter and notify stuff has to do with
    # Python's implementation of Process/Pipe based parallelism. This will
    # probably still be needed for Ractors and Ports, but I don't understand it
    # well enough to add it yet. It will need to wait for another day.
    CANCELLED_AND_NOTIFIED = :CANCELLED_AND_NOTIFIED
    FINISHED = :FINISHED

    private

    def invoke_callbacks
      @done_callbacks.each do |callback|
        callback.call(self)
      rescue Exception # rubocop:disable Lint/RescueException
        logger&.error { "Exception calling callback for #{self}" }
      end
    end
  end

  # Base Executor class. Has a simple implementation that just runs submitted
  # functions immediately and returns a completed Future.
  #
  # Inheriting classes must override the `submit` method.
  class Executor
    def initialize(*_args, **_kwargs)
      if block_given? # rubocop:disable Style/GuardClause
        begin
          yield self
        ensure
          shutdown
        end
      end
    end

    def submit(fun, *args, **kwargs, &) # rubocop:disable Style/ArgumentsForwarding
      future = Future.new
      future.set_running_or_notify_cancel

      begin
        result = fun.call(*args, **kwargs, &) # rubocop:disable Style/ArgumentsForwarding
      rescue Exception => e # rubocop:disable Lint/RescueException
        future.set_exception(e)
      else
        future.set_result(result)
      end

      future
    end

    def map(fun, *iterables, timeout_sec: nil) # rubocop:disable Lint/UnusedMethodArgument
      fs = iterables.map { |args| submit(fun, *args) }

      # FIXME: Need to implement this as an internal enumerator or something so
      # that cleanup can be assured. For example, there needs to be an ensure
      # section to attempt to cancel all futures.
      #
      # See: https://docs.ruby-lang.org/en/3.3/Enumerator.html#class-Enumerator-label-Convert+External+Iteration+to+Internal+Iteration
      # See: https://github.com/python/cpython/blob/59b260c61b5abb75edcb2b0ab901274a58dfc856/Lib/concurrent/futures/_base.py#L612-L625
      # See: https://docs.ruby-lang.org/en/3.3/Enumerable.html#method-i-zip
      fs.lazy.map do |f|
        f.result
      ensure
        f.cancel
      end
    end

    def shutdown(wait = True, cancel_futures = False)
      # In the base implementation, there is nothing to cleanup.
    end
  end
end
