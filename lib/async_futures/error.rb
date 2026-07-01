# frozen_string_literal: true

module AsyncFutures
  # Base error class.
  class Error < StandardError; end

  # Error that signals that concurrency is not available for the requested operation.
  # This may be permanent or temporary.
  class NoConcurrencyError < Error; end

  # Error raised for all invalid Future states.
  class InvalidStateError < Error
    attr_reader :future, :state

    def initialize(future, state)
      @future = future
      @state = state

      super("Unexpected state '#{@state}' for Future: #{@future}")
    end
  end

  # Error that is raised for invalid operations on a cancelled Future.
  class CancelledError < Error; end

  # Error for Executor errors related to Ractors.
  class RactorError < Error; end

  # Error for Future errors related to deadlocks.
  class DeadlockError < Error
    attr_reader :future

    def initialize(future)
      @future = future

      super("Future would deadlock: #{@future}")
    end
  end
end
