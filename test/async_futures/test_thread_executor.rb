# frozen_string_literal: true

require_relative 'minitest_helper'

require 'async_futures/thread_executor'

class TestThreadExecutor < Minitest::Test
  def setup
    @executor = AsyncFutures::ThreadExecutor.new
  end

  def teardown
    @executor.shutdown(wait: true)
  end

  def test_submit_raises_argument_error_without_block
    assert_raises(ArgumentError) { @executor.submit('No block given') }
  end

  def test_submit_returns_a_future_object
    future1 = @executor.submit(1, 2, 3, 4, tell_me: 'that you love me more') do |*args, **kwargs|
      [args, kwargs]
    end

    assert_instance_of AsyncFutures::Future, future1
    assert_predicate future1, :pending?

    result = future1.result

    assert_predicate future1, :done?
    assert_equal 2, result.size
    assert_instance_of Array, result
    assert_instance_of Array, result[0]
    assert_instance_of Hash, result[1]
  end

  def test_submit_raises_returns_exceptional_future
    before_count = Thread.list.size
    future1 = @executor.submit(1, 2, 3, 4, tell_me: 'that you love me more') do |*args, **kwargs|
      raise "Some runtime error #{args} #{kwargs}"
    end
    after_count = Thread.list.size

    assert_operator after_count, :>, before_count
    assert_instance_of AsyncFutures::Future, future1
    assert_predicate future1, :pending?
    refute_nil future1.exception
    assert_predicate future1, :done?
  end

  def test_submit_concurrent_is_alias
    assert_equal @executor.method(:submit), @executor.method(:submit_concurrent)
  end

  def test_map
    enum = [1, 2, 3, 4]
    map_result = @executor.map(enum, &:to_s)

    assert_instance_of Enumerator::Lazy, map_result

    results = map_result.to_a
    first = results.to_a[0]
    last = results.to_a[3]

    assert_instance_of String, first
    assert_equal '1', first
    assert_equal '4', last
  end

  def test_shutdown_without_block
    assert_nil @executor.shutdown
  end

  def test_shutdown_with_block
    refute_nil(@executor.shutdown { true })
  end
end
