# frozen_string_literal: true

require_relative 'minitest_helper'

require 'asynchronous_futures/executor'

class TestExecutor < Minitest::Test
  def setup
    @executor = AsynchronousFutures::Executor
  end

  def test_submit_raises_argument_error_without_block
    assert_raises(ArgumentError) { @executor.submit('No block given') }
  end

  def test_submit_returns_a_future_object
    future1 = @executor.submit(1, 2, 3, 4, tell_me: 'that you love me more') do |*args, **kwargs|
      [args, kwargs]
    end

    assert_instance_of AsynchronousFutures::Future, future1

    # Executor mixin module should run immediately and return a completed future.
    assert_predicate future1, :done?

    result = future1.result

    assert_equal 2, result.size
    assert_instance_of Array, result
    assert_instance_of Array, result[0]
    assert_instance_of Hash, result[1]
  end
end
