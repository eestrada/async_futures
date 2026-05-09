# frozen_string_literal: true

require_relative 'asynchronous_futures/logger'
require 'logger'

# Library to create futures for Ractors, Threads, Fibers, and others.
#
# Set a default logger that logs to $stdout. If you want/need to log somewhere
# else, do the following:
#
#     require 'asynchronous_futures/logger'
#     AsynchronousFutures.logger = Logger.new('/dev/null')
#     require 'asynchronous_futures'
#
# This loads the logger portion, sets it to a value immediately, then loads the
# rest of the library files. In this case the default $stdout logger will not be
# created or set.
#
module AsynchronousFutures
  # Lazily set a default value that just prints to $stdout
  self.logger ||= Logger.new($stdout)
end

# require_relative *after* default logger defined.
require_relative 'asynchronous_futures/version'
require_relative 'asynchronous_futures/error'
require_relative 'asynchronous_futures/future'
require_relative 'asynchronous_futures/executor'
