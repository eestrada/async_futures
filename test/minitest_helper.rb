# frozen_string_literal: true

# SimpleCov will be previously required (and thus defined)
# *only* by the `coverage` rake task.
if defined?(SimpleCov)
  require 'simplecov'
  require 'simplecov-html'
  require 'simplecov-cobertura'

  SimpleCov.start do
    SimpleCov.formatters = [
      SimpleCov::Formatter::HTMLFormatter,
      SimpleCov::Formatter::CoberturaFormatter,
    ]

    add_filter '/test/'

    # This is ignored because SimpleCov doesn't support coverage across Ractors.
    # It also doesn't support ignore Ractor code via magic comments.
    # The only way to ignore currently is by just ignoring an entire file,
    # which is what is done here.
    add_filter '/lib/async_futures/ractor_executor/spawn_worker.rb'

    case RUBY_ENGINE
    when /jruby/
      # No branch coverage for jruby
      minimum_coverage line: 100
    when /truffleruby/
      # No minimum_coverage coverage for truffleruby
      enable_coverage :branch
    else
      # MRI can do everything
      enable_coverage :branch
      minimum_coverage line: 100, branch: 100
    end
  end
end

$LOAD_PATH.unshift File.expand_path('../lib', __dir__)

require 'minitest/mock'
require 'minitest/autorun'
