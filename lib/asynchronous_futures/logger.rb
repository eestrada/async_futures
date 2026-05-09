# frozen_string_literal: true

module AsynchronousFutures # rubocop:disable Style/Documentation
  class << self
    # Configurable logger for the library. All calls assume the standard logger interface.
    attr_accessor :logger
  end
end
