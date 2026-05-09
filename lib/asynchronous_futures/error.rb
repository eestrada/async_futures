# frozen_string_literal: true

require 'timeout'

module AsynchronousFutures
  class Error < StandardError; end

  class ConcurrencyUnavailable < Error; end

  class InvalidStateError < Error; end

  class CancelledError < Error; end

  # Simple alias to Timeout::Error to make this easier to refer to.
  TimeoutError = Timeout::Error
end
