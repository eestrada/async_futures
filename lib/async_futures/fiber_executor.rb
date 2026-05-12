# frozen_string_literal: true

require_relative 'executor'

module AsyncFutures
  class FiberExecutor
    include Executor
  end
end
