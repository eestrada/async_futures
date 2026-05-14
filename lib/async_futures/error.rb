# frozen_string_literal: true

require 'timeout'

module AsyncFutures
  # Base error class.
  class Error < StandardError; end

  # Error that signals that concurrency is not available for the requested operation.
  # This may be permanent or temporary.
  class NoConcurrencyError < Error; end

  # Error raised for all invalid states.
  class InvalidStateError < Error; end

  # Error that is raised for invalid operations on a cancelled Future.
  class CancelledError < Error; end
end
