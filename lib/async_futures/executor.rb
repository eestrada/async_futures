# frozen_string_literal: true

require_relative 'error'
require_relative 'future'

require 'timeout'

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
    # Schedules the block
    # to be executed as `block.call(*args, **kwargs)`
    # and returns a `Future` object representing the execution of the block.
    #
    # Some Executor implementations may,
    # under some or all circumstances,
    # run the given block immediately and synchronously
    # and return an already completed `Future` object.
    def submit(...)
      Future.new.tap { |future| future.complete(...) }
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

    # Similar to `enumerable.map(&block)` except:
    #
    # - `block` is executed asynchronously
    # - several calls to block may be made concurrently
    # - Instead of an `Array`, an `Enumerator::Lazy` is returned
    #
    # Just like `enumerable.map`,
    # args are splatted for the block if there are multiple args.
    # Thus you can do things like this:
    #
    # ```ruby
    # ThreadExecutor.new.map(enum.each_with_index) do |e, i|
    #     [e, i]
    # end
    # ```
    #
    # `Future` instances are joined
    # as the returned `Enumerator::Lazy` is enumerated over
    # via a terminal method like `force`, `to_a`, or `each`.
    # The `Future.result` values,
    # and not the `Future` instances themselves,
    # are what is returned.
    #
    # If a `block` call raises an exception,
    # then that exception will be raised
    # when its value is retrieved when enumerating
    # over the `Enumerator::Lazy` instance.
    # Any remaining `Future` instances will attempt to be cancelled
    # in the case of a raised exception.
    # However, because of possible concurrent execution
    # there is no guarantee that they will be cancelled
    # before being picked up, run, and completed.
    #
    # If `timeout` is given and not `nil`,
    # then execution will raise `Timeout::Error`
    # if more than `timeout` seconds elapses.
    # The elapsed time includes both the initial submission of tasks
    # *and* the enumeration of `Future` results from the returned `Enumerator::Lazy`.
    #
    # Keep in mind that an `Enumerator::Lazy` can be enumerated over more than once
    # *and* that the `timeout` value will be evaluated each time it is enumerated
    # *and* that the timeout value will be calculated from the time of first submission.
    # Thus, enumeration could succeed on the first enumeration,
    # but fail with a `Timeout::Error` on a subsequent enumeration.
    # To avoid timing out on an enumeration after the first enumeration,
    # you should save the result of the first enumeration in an `Array` (or similar)
    # using something like `to_a` on the returned `Enumerator::Lazy` instance.
    # If you immediately enumerate the returned `Enumerator::Lazy` only once
    # or you have passed no `timeout` value,
    # then none of this is a concern.
    #
    # Negative `timeout` values are allowed,
    # but they just raise `Timeout::Error` immediately.
    #
    # Do ***not*** call this method with an infinite `Enumerable`
    # and no `timeout` value:
    # the first thing this method does is force it into a finite collection of futures.
    # An infinite `Enumerable` forced into a finite collection
    # will run forever and eventually eat up all memory.
    def map(enumerable, timeout = nil, &block) # rubocop:disable Metrics/AbcSize,Metrics/CyclomaticComplexity,Metrics/PerceivedComplexity
      timeout = nil if timeout&.zero?

      clock_timeout = Time.now.to_f + timeout if timeout

      futures = []
      begin
        local_timeout = timeout && (clock_timeout - Time.now.to_f)
        raise Timeout::Error unless timeout.nil? || local_timeout.positive?

        Timeout.timeout(local_timeout) do
          enumerable.each { |*args| futures << submit(*args, &block) }
        end
      rescue Exception => e # rubocop:disable Lint/RescueException
        futures.each(&:cancel)
        raise e
      end

      futures.each_with_index.lazy.map do |future, index|
        local_timeout = timeout && (clock_timeout - Time.now.to_f)
        raise Timeout::Error unless timeout.nil? || local_timeout.positive?

        Timeout.timeout(local_timeout) { future.result }
      rescue Exception => e # rubocop:disable Lint/RescueException
        # If *any* future raises an exception,
        # we need to be sure to cancel the remaining ones.
        # It's ok if we call cancel on already completed ones.
        (index...futures.size).each { |i| futures[i].cancel }
        raise e
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
    # or running won't be cancelled, regardless of the value of `cancel_futures`.
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
    # ```ruby
    # ThreadExecutor.new(max_workers: 4).shutdown do |executor|
    #     executor.submit('src1.txt', 'dest1.txt', &FileUtils.method(:cp))
    #     executor.submit('src2.txt', 'dest2.txt', &FileUtils.method(:cp))
    #     executor.submit('src3.txt', 'dest3.txt', &FileUtils.method(:cp))
    #     executor.submit('src4.txt', 'dest4.txt', &FileUtils.method(:cp))
    # end
    # ```
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
