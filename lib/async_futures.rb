# frozen_string_literal: true

# In order of dependency (roughly)
require_relative 'async_futures/logger'
require_relative 'async_futures/version'
require_relative 'async_futures/error'
require_relative 'async_futures/future'
require_relative 'async_futures/executor'
require_relative 'async_futures/fiber_executor'
require_relative 'async_futures/ractor_executor'
require_relative 'async_futures/thread_executor'

# Library to create Future instances.
# Has Executor implementations for for Ractor, Thread, and Fiber primitives.
module AsyncFutures
end
