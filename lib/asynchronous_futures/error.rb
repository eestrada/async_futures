# frozen_string_literal: true

require 'timeout'

module AsynchronousFutures
  class Error < StandardError; end

  class ConcurrencyUnavailable < Error; end

  class InvalidStateError < Error; end

  class CancelledError < Error; end
end
