# frozen_string_literal: true

require_relative 'executor'

module AsyncFutures
  class ThreadExecutor
    include Executor
  end
end
