# frozen_string_literal: true

require_relative 'async_futures/logger'
require 'logger'

# Library to create futures for Ractors, Threads, Fibers, and others.
#
# Set a default logger that logs to $stdout. If you want/need to log somewhere
# else, do the following:
#
#     require 'async_futures/logger'
#     AsyncFutures.logger = Logger.new('/dev/null')
#     require 'async_futures'
#
# This loads the logger portion, sets it to a value immediately, then loads the
# rest of the library files. In this case the default $stdout logger will not be
# created or set.
#
module AsyncFutures
  # Lazily set a default value that just prints to $stdout
  self.logger ||= Logger.new($stdout)
end

# require_relative *after* default logger defined.
# In order of dependency (roughly)
require_relative 'async_futures/version'
require_relative 'async_futures/error'
require_relative 'async_futures/future'
require_relative 'async_futures/executor'
require_relative 'async_futures/fiber_executor'
require_relative 'async_futures/ractor_executor'
require_relative 'async_futures/thread_executor'
