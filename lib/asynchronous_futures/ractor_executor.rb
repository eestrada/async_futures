# frozen_string_literal: true

require_relative 'executor'

module AsynchronousFutures
  class RactorExecutor
    include Executor
  end
end
