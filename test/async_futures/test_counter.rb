# frozen_string_literal: true

require_relative 'minitest_helper'

require 'async_futures/counter'

class TestCounter < Minitest::Test
  def setup
    @counter = AsyncFutures::Counter.new
  end

  def test_value
    assert_equal 0, @counter.value
    @counter.increment

    assert_equal 1, @counter.value
  end

  def test_increment
    assert_equal 0, @counter.value
    assert_equal 1, @counter.increment
    assert_equal 1, @counter.value
  end
end
