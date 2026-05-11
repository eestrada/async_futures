# frozen_string_literal: true

# SimpleCov will be previously required (and thus defined) *only* by the
# `coverage` rake task.
if defined?(SimpleCov)
  require 'simplecov'
  require 'simplecov-html'
  require 'simplecov-cobertura'

  SimpleCov.start do
    SimpleCov.formatters = [
      SimpleCov::Formatter::HTMLFormatter,
      SimpleCov::Formatter::CoberturaFormatter,
    ]

    enable_coverage :branch
    add_filter '/test/'
  end
end

$LOAD_PATH.unshift File.expand_path('../lib', __dir__)

require 'minitest/autorun'
