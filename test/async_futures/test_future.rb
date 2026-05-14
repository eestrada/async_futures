# frozen_string_literal: true

require_relative 'minitest_helper'

require 'async_futures/future'

class TestFuture < Minitest::Test
  def test_new_future_should_be_pending
    future1 = AsyncFutures::Future.new

    assert_predicate future1, :pending?
    refute_predicate future1, :running?
  end

  def test_future_should_be_running_after_set_notify
    future1 = AsyncFutures::Future.new

    refute_predicate future1, :running?

    future1.set_running_or_notify_cancel

    assert_predicate future1, :running?
  end

  def test_set_result_cannot_be_called_twice
    future1 = AsyncFutures::Future.new
    future1.set_running_or_notify_cancel
    future1.set_result(42)
    raised_exc = assert_raises(AsyncFutures::InvalidStateError) { future1.set_result(43) }
    assert_match(/FINISHED: /, raised_exc.message)
    assert_equal 42, future1.result
  end

  def test_set_exception_cannot_be_called_twice
    future1 = AsyncFutures::Future.new
    future1.set_running_or_notify_cancel
    test_exception = RuntimeError.new('A test exception')
    future1.set_exception(test_exception)
    raised_exc = assert_raises(AsyncFutures::InvalidStateError) { future1.set_exception(test_exception) }
    assert_match(/FINISHED: /, raised_exc.message)
  end

  def test_set_exception_causes_result_to_raise_exception
    future1 = AsyncFutures::Future.new
    future1.set_running_or_notify_cancel
    test_exception = RuntimeError.new('A test exception')
    future1.set_exception(test_exception)
    raised_exc = assert_raises(RuntimeError) { future1.result }
    assert_same test_exception, raised_exc
  end

  def test_cancel_causes_result_to_raise_cancelled_error
    future1 = AsyncFutures::Future.new
    future1.cancel
    raised_exc = assert_raises(AsyncFutures::CancelledError) { future1.result }
    assert_instance_of AsyncFutures::CancelledError, raised_exc
  end

  def test_cancel_causes_exception_to_raise_cancelled_error
    future1 = AsyncFutures::Future.new
    future1.cancel
    raised_exc = assert_raises(AsyncFutures::CancelledError) { future1.exception }
    assert_instance_of AsyncFutures::CancelledError, raised_exc
  end

  def test_cancel_can_be_called_multiple_times
    future1 = AsyncFutures::Future.new

    refute_predicate future1, :cancelled?
    assert_equal true, future1.cancel # rubocop:disable Minitest/AssertTruthy
    assert_predicate future1, :cancelled?
    assert_equal true, future1.cancel # rubocop:disable Minitest/AssertTruthy
    assert_predicate future1, :cancelled?
  end

  def test_calling_set_running_twice_raises_exception
    future1 = AsyncFutures::Future.new
    future1.set_running_or_notify_cancel
    raised_exc = assert_raises(AsyncFutures::InvalidStateError) { future1.set_running_or_notify_cancel }
    assert_same 'Future in unexpected state', raised_exc.message
  end
end
