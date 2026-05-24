# frozen_string_literal: true

require_relative 'minitest_helper'

require 'async_futures/executor'

class TestExecutor < Minitest::Test
  def setup
    @executor = AsyncFutures::Executor
  end

  def test_submit_raises_argument_error_without_block
    assert_raises(ArgumentError) { @executor.submit('No block given') }
  end

  def test_submit_returns_a_future_object
    future1 = @executor.submit(1, 2, 3, 4, tell_me: 'that you love me more') do |*args, **kwargs|
      [args, kwargs]
    end

    assert_instance_of AsyncFutures::Future, future1

    # Executor mixin module should run immediately and return a completed future.
    assert_predicate future1, :done?

    result = future1.result

    assert_equal 2, result.size
    assert_instance_of Array, result
    assert_instance_of Array, result[0]
    assert_instance_of Hash, result[1]
  end

  def test_submit_raises_returns_exceptional_future
    future1 = @executor.submit(1, 2, 3, 4, tell_me: 'that you love me more') do |*args, **kwargs|
      raise "Some runtime error #{args} #{kwargs}"
    end

    assert_instance_of AsyncFutures::Future, future1

    # Executor mixin module should run immediately and return a completed future.
    assert_predicate future1, :done?

    refute_nil future1.exception
  end

  def test_submit_concurrent_raises
    assert_raises(AsyncFutures::NoConcurrencyError) do
      @executor.submit_concurrent(1, 2, 3, 4, tell_me: 'that you love me more') do |*args, **kwargs|
        raise "Some runtime error #{args} #{kwargs}"
      end
    end
  end

  def test_map
    enum = [1, 2, 3, 4]
    map_result = @executor.map(enum, &:to_s)

    assert_instance_of Enumerator::Lazy, map_result

    first = map_result.to_a[0]
    last = map_result.to_a[3]

    assert_instance_of String, first
    assert_equal '1', first
    assert_equal '4', last
  end

  def test_map_exception
    enum = [1, 2, 3, 4]
    map_result = @executor.map(enum) { |i| i == 3 ? (raise 'fail on 3') : i.to_s }

    assert_instance_of Enumerator::Lazy, map_result

    exc = assert_raises(RuntimeError) { map_result.to_a }

    assert_match(/^fail on 3$/, exc.message)
  end

  def test_map_timeout_short
    enum = [0.05, 0.05]
    map_result = @executor.map(enum, 0.05) { |s| sleep(s) }

    assert_instance_of Enumerator::Lazy, map_result

    assert_raises(Timeout::Error) { map_result.to_a }
  end

  def test_map_timeout_long
    enum = [0.05, 0.05]
    map_result = @executor.map(enum, 0.15) { |s| sleep(s) }

    assert_instance_of Enumerator::Lazy, map_result

    ary_result = map_result.to_a

    assert_instance_of Array, ary_result
  end

  def test_map_timeout_zero
    enum = [0.05, 0.05]
    map_result = @executor.map(enum, 0.0) do |s|
      sleep(s)
      s.floor(-1)
    end

    assert_instance_of Enumerator::Lazy, map_result

    # won't raise Timeout::Error
    ary = map_result.to_a

    assert_instance_of Array, ary

    assert_equal [0, 0], ary
  end

  def test_shutdown_without_block
    assert_nil @executor.shutdown
  end

  def test_shutdown_with_block
    refute_nil(@executor.shutdown { true })
  end
end
