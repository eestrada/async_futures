# frozen_string_literal: true

require 'delegate'

module AsyncFutures
  # A Delegator that synchronizes all calls for a delegated object.
  #
  # This does not guarantee concurrency safety under all circumstances,
  # For example, enumeration/iteration may not be totally safe.
  # But it does make it easier to be concurrency safe.
  #
  # For example,
  # you can do a concurrency safe copy of the object,
  # then enumerate over it.
  #
  # The only argument to `new` is  the object to be wrapped.
  # Obviously, it shouldn't be directly accessed
  # outside the delegator after initial creation.
  class SynchronizedDelegator < SimpleDelegator
    def initialize(obj)
      super
      @mutex = Mutex.new
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
  end
end
