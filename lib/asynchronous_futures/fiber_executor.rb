# frozen_string_literal: true

require_relative 'executor'

module AsynchronousFutures
  class FiberExecutor
    include Executor
  end
end
