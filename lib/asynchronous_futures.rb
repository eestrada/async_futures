# frozen_string_literal: true

require_relative 'asynchronous_futures/version'
require 'timeout'
require 'monitor'

# Library to create futures for Ractors, Threads, and Fibers.
module AsynchronousFutures
  # Configurable logger for the library. Assumes the standard logger interface.
  attr_accessor :logger

  class Error < StandardError; end

  class ConcurrencyUnavailable < Error; end

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

  # Executor mixin module. Has a simple implementation that just runs submitted
  # functions immediately and returns a completed Future. Can be used standalone
  # as a stateless Executor that runs submitted blocks immediately.
  #
  # Classes using this mixin should override the `submit` method.
  #
  # `shutdown` should be overridden if there is cleanup to be performed.
  #
  # If an implementation wants to signal that it supports true concurrency, it
  # should override `submit_concurrent`; this can be as simple as aliasing it to
  # the previously overridden `submit` method.
  #
  # The `map` method should *never* be overridden. This is already logically
  # correct and should work with any Executor implementation.
  module Executor
    # Schedules the block, to be executed as `block.call(*args, **kwargs)` and
    # returns a `Future` object representing the execution of the block.
    def submit(*args, **kwargs, &block) # rubocop:disable Style/ArgumentsForwarding
      raise ArgumentError.new('No block given') unless block

      future = Future.new
      future.set_running_or_notify_cancel

      begin
        result = block.call(*args, **kwargs) # rubocop:disable Style/ArgumentsForwarding
      rescue Exception => e # rubocop:disable Lint/RescueException
        future.set_exception(e)
      else
        future.set_result(result)
      end

      future
    end

    # Schedules the block, to be executed as `block.call(*args, **kwargs)` and
    # returns a `Future` object representing the execution of the block.
    #
    # Executor must support concurrency otherwise this method will raise the
    # exception `ConcurrencyUnavailable`.
    def submit_concurrent(*_args, **_kwargs, &)
      raise ConcurrencyUnavailable
    end

    # Similar to map(fn, *enumerator) except:
    #
    # the enumerator are collected immediately rather than lazily;
    #
    # fn is executed asynchronously and several calls to fn may be made
    # concurrently.
    #
    # The returned iterator raises a TimeoutError if __next__() is called and
    # the result isn’t available after timeout seconds from the original call to
    # Executor.map(). timeout can be an int or a float. If timeout is not
    # specified or None, there is no limit to the wait time.
    #
    # If a fn call raises an exception, then that exception will be raised when
    # its value is retrieved from the iterator.
    #
    # When using ProcessPoolExecutor, this method chops enumerator into a number
    # of chunks which it submits to the pool as separate tasks. The
    # (approximate) size of these chunks can be specified by setting chunksize
    # to a positive integer. For very long enumerator, using a large value for
    # chunksize can significantly improve performance compared to the default
    # size of 1. With ThreadPoolExecutor, chunksize has no effect.
    def map(*enumerator, timeout_sec: nil, &block) # rubocop:disable Lint/UnusedMethodArgument
      # Use `to_a` in case the enumerator is lazy (we *want* to be eager in this
      # circumstance).
      futures = enumerator.map { |args| submit(args, &block) }.to_a

      # FIXME: Need to implement this as an internal enumerator or something so
      # that cleanup can be assured and we can support timeouts. For example,
      # there needs to be an ensure section to attempt to cancel all futures.
      #
      # See: https://docs.ruby-lang.org/en/3.3/Enumerator.html#class-Enumerator-label-Convert+External+Iteration+to+Internal+Iteration
      # See: https://github.com/python/cpython/blob/59b260c61b5abb75edcb2b0ab901274a58dfc856/Lib/concurrent/futures/_base.py#L612-L625
      # See: https://docs.ruby-lang.org/en/3.3/Enumerable.html#method-i-zip
      futures.lazy.map do |f|
        f.result
      ensure
        f.cancel
      end
    end

    # Signal the executor that it should free any resources that it is using
    # when the currently pending futures are done executing. Calls to
    # Executor.submit() and Executor.map() made after shutdown will raise
    # RuntimeError.
    #
    # If wait is True then this method will not return until all the pending
    # futures are done executing and the resources associated with the executor
    # have been freed. If wait is False then this method will return immediately
    # and the resources associated with the executor will be freed when all
    # pending futures are done executing. Regardless of the value of wait, the
    # entire Python program will not exit until all pending futures are done
    # executing.
    #
    # If cancel_futures is True, this method will cancel all pending futures
    # that the executor has not started running. Any futures that are completed
    # or running won’t be cancelled, regardless of the value of cancel_futures.
    #
    # If both cancel_futures and wait are True, all futures that the executor
    # has started running will be completed prior to this method returning. The
    # remaining futures are cancelled.
    #
    # You can ensure this gets called under all circumstances by calling this
    # method with a block. The block will be called and then any shutdown
    # cleanup logic will be run after the block completes. The block will be
    # passed one parameter: the executor instance.
    #
    # ThreadExecutor.new(max_workers=4).shutdown do |e|
    #     e.submit('src1.txt', 'dest1.txt', &FileUtils.cp)
    #     e.submit('src2.txt', 'dest2.txt', &FileUtils.cp)
    #     e.submit('src3.txt', 'dest3.txt', &FileUtils.cp)
    #     e.submit('src4.txt', 'dest4.txt', &FileUtils.cp)
    # end
    def shutdown(wait = True, cancel_futures = False, &block) # rubocop:disable Lint/UnusedMethodArgument
      # In the base implementation, there is nothing to cleanup.
      block&.call(self)
    ensure # rubocop:disable Lint/EmptyEnsure
      # Cleanup logic goes here
      #
      # The mixin has no state, so it has nothing to cleanup.
    end

    module_function :submit, :submit_concurrent, :map, :shutdown
  end
end
