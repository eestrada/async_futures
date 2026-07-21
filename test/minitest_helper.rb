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

    enable_for_subprocesses true
    add_filter '/test/'

    case RUBY_ENGINE
    when /jruby/
      # No branch coverage or minimum_coverage for jruby
      # minimum_coverage line: 100
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
