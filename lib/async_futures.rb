# frozen_string_literal: true

# Library to create Future instances.
#
# Has Executor implementations for Ractor, Thread, and Fiber primitives.
module AsyncFutures
end

# In order of dependency (roughly)
require_relative 'async_futures/logger'
require_relative 'async_futures/version'
require_relative 'async_futures/error'
require_relative 'async_futures/future'
require_relative 'async_futures/executor'
require_relative 'async_futures/fiber_executor'
require_relative 'async_futures/thread_executor'

# ractor executor is only support in version 4.x or greater,
# so it must be required explicitly.
# require_relative 'async_futures/ractor_executor'
