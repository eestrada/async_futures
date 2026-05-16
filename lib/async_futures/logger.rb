# frozen_string_literal: true

require 'logger'

module AsyncFutures # rubocop:disable Style/Documentation
  class << self
    # Configurable logger for the library. All calls assume the standard logger interface.
    #
    # Defaults to being unset (i.e. `nil`).
    attr_accessor :logger
  end
end
