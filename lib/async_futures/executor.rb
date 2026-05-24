# frozen_string_literal: true

require_relative 'error'
require_relative 'future'

module AsyncFutures
  # `Executor` mixin module.
  # Has a simple implementation
  # that just runs submitted blocks immediately
  # and returns a completed `Future`.
  # Can be used standalone as a stateless `Executor`
  # that runs submitted blocks immediately.
  #
  # Classes using this mixin should override the `submit` method.
  #
  # `shutdown` should be overridden if there is cleanup to be performed.
  #
  # If an implementation wants to signal that it supports true concurrency,
  # it should override the `submit_concurrent` method;
  # this can be as simple as aliasing it
  # to the previously overridden `submit` method.
  #
  # The `map` method should *never* be overridden.
  # This is already logically correct
  # and should work with any `Executor` implementation.
  module Executor
    # Schedules the block, to be executed as `block.call(*args, **kwargs)`
    # and returns a `Future` object representing the execution of the block.
    #
    # Under some circumstances may run immediately and synchronously
    # and return an already completed `Future` object.
    def submit(*args, **kwargs, &block)
      raise ArgumentError.new('No block given') unless block

      Future.new.tap do |future|
        future.set_running_or_notify_cancel

        begin
          result = block.call(*args, **kwargs)
        rescue Exception => e # rubocop:disable Lint/RescueException
          future.set_exception(e)
        else
          future.set_result(result)
        end
      end
    end

    # Schedules the block, to be executed as `block.call(*args, **kwargs)` and
    # returns a `Future` object representing the execution of the block.
    #
    # Executor must support concurrency otherwise this method will raise the
    # exception `NoConcurrencyError`.
    #
    # This method should *never* run the block to completion before returning.
    # This could cause a serious deadlock condition that cannot be overcome.
    # If an implementation cannot schedule this to run concurrently
    # it is better for it to raise an exception such as `NoConcurrencyError`.
    # This at least allows the caller an opportunity to recover
    # instead of potentially deadlocking.
    def submit_concurrent(*args, **kwargs, &block) # rubocop:disable Lint/UnusedMethodArgument,Naming/BlockForwarding
      raise NoConcurrencyError
    end

    # Similar to `enumerator.map(&block)` except:
    # `block` is executed asynchronously
    # and several calls to block may be made concurrently.
    #
    # An `Enumerator::Lazy` instance will be returned.
    # `Future` instances are joined
    # as the `Enumerator::Lazy` is enumerated over.
    #
    # If a `block` call raises an exception,
    # then that exception will be raised
    # when its value is retrieved from the `Enumerator::Lazy` instance.
    def map(enumerator, timeout_sec: nil, &block) # rubocop:disable Lint/UnusedMethodArgument
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
    # If `wait` is `true` then this method will not return until all the pending
    # futures are done executing and the resources associated with the executor
    # have been freed. If `wait` is `False` then this method will return immediately
    # and the resources associated with the executor will be freed when all
    # pending futures are done executing. Regardless of the value of `wait`, the
    # entire Ruby program will not exit until all pending futures are done
    # executing.
    #
    # If `cancel_futures` is `true`, this method will cancel all pending futures
    # that the executor has not started running. Any futures that are completed
    # or running won’t be cancelled, regardless of the value of `cancel_futures`.
    #
    # If both `cancel_futures` and `wait` are `true`, all futures that the executor
    # has started running will be completed prior to this method returning. The
    # remaining futures are cancelled.
    #
    # You can ensure this gets called under all circumstances
    # by calling this method with a block.
    # The block will be called
    # and then any shutdown cleanup logic will be run
    # after the block completes.
    # The block will be passed one parameter: the executor instance.
    #
    #     ThreadExecutor.new(max_workers: 4).shutdown do |executor|
    #         executor.submit('src1.txt', 'dest1.txt', &FileUtils.method(:cp))
    #         executor.submit('src2.txt', 'dest2.txt', &FileUtils.method(:cp))
    #         executor.submit('src3.txt', 'dest3.txt', &FileUtils.method(:cp))
    #         executor.submit('src4.txt', 'dest4.txt', &FileUtils.method(:cp))
    #     end
    #
    # `shutdown` can be called multiple times.
    # The block given will always be run,
    # but the actual procedure to shutdown afterward will only be called once,
    # on the first time.
    #
    # It is the caller's responsibility
    # to ensure that the passed block can deal with a shutdown executor
    # if there is any possibility
    # of `shutdown` being called more than once with a block.
    # Unless the caller is doing something very out of the ordinary,
    # this is unlikely to be an issue.
    #
    # This method returns the return value of the block,
    # or `nil` if no block is given.
    def shutdown(wait: true, cancel_futures: false, &block) # rubocop:disable Lint/UnusedMethodArgument
      block&.call(self)
    ensure # rubocop:disable Lint/EmptyEnsure
      # Cleanup logic goes here.
      #
      # The mixin has no state,
      # so it has nothing to cleanup.
      #
      # Also, this is the only implementation that will *not* raise
      # an exception when new tasks are submitted after shutdown,
      # precisely because it has no state
      # to even keep track of whether shutdown has previously been called or not.
    end

    module_function :submit, :submit_concurrent, :map, :shutdown

    public :submit, :submit_concurrent, :map, :shutdown
  end
end
