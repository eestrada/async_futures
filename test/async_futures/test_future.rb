# frozen_string_literal: true

require_relative 'minitest_helper'

require 'async_futures/future'

class TestFuture < Minitest::Test # rubocop:disable Metrics/ClassLength
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

  def test_set_result_invokes_callbacks
    future1 = AsyncFutures::Future.new
    future1.set_running_or_notify_cancel
    ary = []
    future1.add_done_callback do |f|
      ary << 'callback1'

      assert_same future1, f
    end
    future1.add_done_callback { ary << 'callback2' }

    assert_empty ary
    future1.set_result(42)

    refute_empty ary
    assert_equal %w[callback1 callback2], ary
  end

  def test_adding_callback_after_completion_still_calls_callback
    future1 = AsyncFutures::Future.new
    future1.set_running_or_notify_cancel
    future1.set_result(42)
    ary = []
    future1.add_done_callback do
      ary << 'callback1'
      raise 'Any error'
    end
    future1.add_done_callback { ary << 'callback2' }

    refute_empty ary
    assert_equal %w[callback1 callback2], ary
  end

  def test_late_callbacks_that_raise_will_log_errors
    future1 = AsyncFutures::Future.new
    future1.set_running_or_notify_cancel
    future1.set_result(42)

    @mock = Minitest::Mock.new
    AsyncFutures.logger = @mock
    @mock.expect :error, nil

    ary = []
    future1.add_done_callback do
      raise 'Any error'
      ary << 'callback1' # rubocop:disable Lint/UnreachableCode
    end
    future1.add_done_callback { ary << 'callback2' }

    refute_empty ary
    assert_equal %w[callback2], ary

    @mock.verify
  ensure
    AsyncFutures.logger = nil
  end

  def test_add_callback_requires_block
    future1 = AsyncFutures::Future.new
    future1.set_running_or_notify_cancel
    exc = assert_raises(ArgumentError) { future1.add_done_callback }
    assert_match(/No block given/, exc.message)
  end

  def test_callbacks_that_raise_will_log_errors
    future1 = AsyncFutures::Future.new
    future1.set_running_or_notify_cancel
    ary = []
    future1.add_done_callback do
      raise 'Any error'
      ary << 'callback1' # rubocop:disable Lint/UnreachableCode
    end
    future1.add_done_callback { ary << 'callback2' }

    assert_empty ary

    @mock = Minitest::Mock.new
    AsyncFutures.logger = @mock
    @mock.expect :error, nil

    # should not raise any exceptions
    future1.set_result(42)

    refute_empty ary
    assert_equal %w[callback2], ary

    @mock.verify
  ensure
    AsyncFutures.logger = nil
  end

  def test_invokes_callbacks_does_not_raise
    future1 = AsyncFutures::Future.new
    future1.set_running_or_notify_cancel
    ary = []
    future1.add_done_callback do
      ary << 'callback1'
      raise 'Any error'
    end
    future1.add_done_callback { ary << 'callback2' }

    assert_empty ary

    # should not raise any exceptions
    future1.set_result(42)

    refute_empty ary
    assert_equal %w[callback1 callback2], ary
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

  def test_set_exception_without_exception_raises_argument_error
    future1 = AsyncFutures::Future.new
    future1.set_running_or_notify_cancel
    not_exception = 'Some random value'
    raised_exc = assert_raises(ArgumentError) { future1.set_exception(not_exception) }
    assert_match(/"Some random value"/, raised_exc.message)
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
    assert_same true, future1.cancel
    assert_predicate future1, :cancelled?
    assert_same true, future1.cancel
    assert_predicate future1, :cancelled?
  end

  def test_calling_set_running_twice_logs_properly
    @mock = Minitest::Mock.new
    AsyncFutures.logger = @mock
    @mock.expect :unknown, nil

    future1 = AsyncFutures::Future.new
    future1.set_running_or_notify_cancel
    raised_exc = assert_raises(AsyncFutures::InvalidStateError) { future1.set_running_or_notify_cancel }
    assert_same 'Future in unexpected state', raised_exc.message

    @mock.verify
  ensure
    AsyncFutures.logger = nil
  end

  def test_calling_set_running_twice_raises_exception
    future1 = AsyncFutures::Future.new
    future1.set_running_or_notify_cancel
    raised_exc = assert_raises(AsyncFutures::InvalidStateError) { future1.set_running_or_notify_cancel }
    assert_same 'Future in unexpected state', raised_exc.message
  end

  def test_calling_set_running_after_cancel_returns_false
    future1 = AsyncFutures::Future.new
    future1.cancel
    value1 = future1.set_running_or_notify_cancel

    assert_same false, value1
  end

  def test_test_fiber_and_future_attrs
    future1 = AsyncFutures::Future.new

    assert_nil future1.fiber
    assert_nil future1.thread

    future1.fiber = Fiber.current
    future1.thread = Thread.current

    assert_equal Fiber.current, future1.fiber
    assert_equal Thread.current, future1.thread
  end

  def test_finished_predicate
    future1 = AsyncFutures::Future.new

    refute_predicate future1, :finished?

    future1.set_running_or_notify_cancel
    future1.set_result(true)

    assert_predicate future1, :finished?
  end

  def test_join_deadlock_on_same_fiber
    future1 = AsyncFutures::Future.new
    future1.set_running_or_notify_cancel

    future1.fiber = Fiber.current

    exc = assert_raises(AsyncFutures::DeadlockError) { future1.join }

    assert_match(/^Future would deadlock: #<AsyncFutures::Future:\w+>$/, exc.message)
  end

  def test_join_deadlock_on_same_fiber_wont_raise_on_done
    future1 = AsyncFutures::Future.new
    future1.set_running_or_notify_cancel

    future1.fiber = Fiber.current

    future1.set_result(true)

    assert_equal future1, future1.join(0.01)
  end

  def test_join_timeout_zero_returns_nil
    future1 = AsyncFutures::Future.new
    future1.set_running_or_notify_cancel

    assert_nil future1.join(0)
  end

  def test_join_timeout_non_zero_returns_nil
    future1 = AsyncFutures::Future.new
    future1.set_running_or_notify_cancel

    assert_nil future1.join(0.01)
  end
end
