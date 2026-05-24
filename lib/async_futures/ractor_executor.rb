# frozen_string_literal: true

# The Ractor API was different before version 4.x of Ruby.
unless /^4\./ === RUBY_VERSION
  raise LoadError.new("'async_futures/ractor_executor' is not supported in Ruby version '#{RUBY_VERSION}'")
end

require_relative 'executor'

module AsyncFutures
  # `Executor` implementation based on `Ractor` primitives
  # that uses a pool of up to `max_workers` to execute calls concurrently.
  #
  # Only available in Ruby version 4.0 and higher.
  # Requiring this file in earlier versions will raise a `LoadError`.
  class RactorExecutor
    include Executor
  end
end
