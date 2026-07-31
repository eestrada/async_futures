# frozen_string_literal: true

require 'delegate'

module AsyncFutures
  # A Delegator that synchronizes all calls for a delegated object.
  #
  # This does not guarantee concurrency safety under all circumstances,
  # but it does make it easier to be safe.
  # For example,
  # any method that returns `self` is potentially unsafe if chained afterward
  # (because the chained operations can happen outside the synchronized mutex,
  # and thus are not safe for concurrency).
  #
  # The only argument to `new` is  the object to be wrapped.
  # For concurrency safety, it should no longer be directly accessed
  # outside the delegator
  # unless you are know you are in a situation
  # where the object will not be accessed concurrently.
  class SynchronizedDelegator < SimpleDelegator
    def initialize(obj)
      super
      @mutex = Thread::Mutex.new
    end

    # Like regular `method_missing`, but all calls are synchronized.
    def method_missing(name, *args, **kwargs, &)
      @mutex.synchronize do
        # SimpleDelegator#method_missing forwards to __getobj__
        super
      end
    end

    def respond_to_missing?(name, include_private = false)
      # keep respond_to? consistent
      super
    end

    # A synchronized implementation of `clone`.
    #
    # Uses a custom implementation of `initialize_clone`
    # that creates and assigns a new mutex
    # to the clone.
    # The internal mutex will also be properly frozen
    # if the original delegator is frozen.
    def clone(freeze: nil)
      @mutex.synchronize do
        super
      end
    end

    # A synchronized implementation of `dup`.
    #
    # Uses a custom implementation of `initialize_dup`
    # that creates and assigns a new mutex
    # to the duplicate.
    def dup
      @mutex.synchronize do
        super
      end
    end

    # A synchronized implementation of `freeze`.
    #
    # Also freezes private mutex.
    def freeze
      @mutex.synchronize do
        @mutex.freeze
        super
      end
    end

    private

    def initialize_dup(other) # :nodoc:
      @mutex = other.instance_variable_get(:@mutex).dup
      super
    end

    def initialize_clone(other, freeze: nil) # :nodoc:
      @mutex = other.instance_variable_get(:@mutex).clone(freeze: freeze)
      super
    end
  end
end
