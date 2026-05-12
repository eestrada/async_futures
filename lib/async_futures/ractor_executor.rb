# frozen_string_literal: true

require_relative 'executor'

module AsyncFutures
  class RactorExecutor
    include Executor
  end
end
