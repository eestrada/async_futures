# frozen_string_literal: true

module AsyncFutures
  # Simple counter.
  # Useful to wrap with a SynchronizedDelegator.
  class Counter
    attr_reader :value

    def initialize(value = 0)
      @value = value
    end

    def increment(amount = 1)
      @value += amount
    end
  end
end
