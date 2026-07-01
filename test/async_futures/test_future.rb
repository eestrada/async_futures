# frozen_string_literal: true

require_relative 'minitest_helper'

require 'async_futures/future'

class TestFuture < Minitest::Test # rubocop:disable Metrics/ClassLength
  def test_wait # rubocop:disable Metrics/AbcSize
    future1 = AsyncFutures::Future.new
    future2 = AsyncFutures::Future.new
    future3 = AsyncFutures::Future.new

    all_fs = [future1, future2, future3]
    dup_fs = [future1, future2, future3, future1]

    all_fs.each(&:set_running_or_notify_cancel)
    all_fs.each_with_index { |f, i| f.set_result(i) }

    completed_hsh = AsyncFutures::Future.wait(dup_fs)

    assert_instance_of Hash, completed_hsh

    assert_includes completed_hsh, :done
    assert_includes completed_hsh, :not_done

    assert_instance_of Set, completed_hsh[:done]
    assert_instance_of Set, completed_hsh[:not_done]

    assert_equal all_fs.size, completed_hsh[:done].size
    assert_equal 0, completed_hsh[:not_done].size

    assert_equal all_fs.to_set, completed_hsh[:done]
  end

  def test_wait_empty
    completed_hsh = AsyncFutures::Future.wait([])

    assert_instance_of Hash, completed_hsh

    assert_includes completed_hsh, :done
    assert_includes completed_hsh, :not_done

    assert_equal 0, completed_hsh[:done].size
    assert_equal 0, completed_hsh[:not_done].size
  end

  def test_wait_with_bad_return_when_value
    # An empty array would return too early.
    # So fill it with one future.
    future1 = AsyncFutures::Future.new
    future1.set_running_or_notify_cancel
    future1.set_result(true)

    fs = [future1]

    exc = assert_raises(ArgumentError) { AsyncFutures::Future.wait(fs, nil, :bad_value) }

    assert_match(/^Unknown 'return_when' value 'bad_value'$/, exc.message)
  end

  def test_wait_partial_completion_with_timeout_short # rubocop:disable Metrics/AbcSize
    future1 = AsyncFutures::Future.new
    future2 = AsyncFutures::Future.new
    future3 = AsyncFutures::Future.new

    all_fs = [future1, future2, future3]
    dup_fs = [future1, future2, future3, future1]

    all_fs.each(&:set_running_or_notify_cancel)
    all_fs.take(2).each_with_index { |f, i| f.set_result(i) }

    completed_hsh = AsyncFutures::Future.wait(dup_fs, 0.01)

    assert_instance_of Hash, completed_hsh

    assert_includes completed_hsh, :done
    assert_includes completed_hsh, :not_done

    assert_instance_of Set, completed_hsh[:done]
    assert_instance_of Set, completed_hsh[:not_done]

    assert_equal 2, completed_hsh[:done].size
    assert_equal 1, completed_hsh[:not_done].size

    assert_equal all_fs.take(2).to_set, completed_hsh[:done]
  end

  def test_wait_partial_completion_with_timeout_negative # rubocop:disable Metrics/AbcSize
    future1 = AsyncFutures::Future.new
    future2 = AsyncFutures::Future.new
    future3 = AsyncFutures::Future.new

    all_fs = [future1, future2, future3]
    dup_fs = [future1, future2, future3, future1]

    all_fs.each(&:set_running_or_notify_cancel)
    all_fs.take(2).each_with_index { |f, i| f.set_result(i) }

    completed_hsh = AsyncFutures::Future.wait(dup_fs, -0.01)

    assert_instance_of Hash, completed_hsh

    assert_includes completed_hsh, :done
    assert_includes completed_hsh, :not_done

    assert_instance_of Set, completed_hsh[:done]
    assert_instance_of Set, completed_hsh[:not_done]

    assert_equal 2, completed_hsh[:done].size
    assert_equal 1, completed_hsh[:not_done].size

    assert_equal all_fs.take(2).to_set, completed_hsh[:done]
  end

  def test_wait_first_completed # rubocop:disable Metrics/AbcSize
    future1 = AsyncFutures::Future.new
    future2 = AsyncFutures::Future.new
    future3 = AsyncFutures::Future.new

    all_fs = [future1, future2, future3]
    dup_fs = [future1, future2, future3, future1]

    all_fs.each(&:set_running_or_notify_cancel)

    future2.set_result(1)

    completed_hsh = AsyncFutures::Future.wait(dup_fs, nil, AsyncFutures::Future::FIRST_COMPLETED)

    assert_instance_of Hash, completed_hsh

    assert_includes completed_hsh, :done
    assert_includes completed_hsh, :not_done

    assert_instance_of Set, completed_hsh[:done]
    assert_instance_of Set, completed_hsh[:not_done]

    assert_equal 1, completed_hsh[:done].size
    assert_equal 2, completed_hsh[:not_done].size

    assert_equal [future2].to_set, completed_hsh[:done]
  end

  def test_wait_first_completed_multiple # rubocop:disable Metrics/AbcSize
    future1 = AsyncFutures::Future.new
    future2 = AsyncFutures::Future.new
    future3 = AsyncFutures::Future.new

    all_fs = [future1, future2, future3]
    dup_fs = [future1, future2, future3, future1]

    all_fs.each(&:set_running_or_notify_cancel)

    future2.set_result(1)
    future3.set_result(2)

    completed_hsh = AsyncFutures::Future.wait(dup_fs, nil, AsyncFutures::Future::FIRST_COMPLETED)

    assert_instance_of Hash, completed_hsh

    assert_includes completed_hsh, :done
    assert_includes completed_hsh, :not_done

    assert_instance_of Set, completed_hsh[:done]
    assert_instance_of Set, completed_hsh[:not_done]

    assert_equal 2, completed_hsh[:done].size
    assert_equal 1, completed_hsh[:not_done].size

    assert_equal [future2, future3].to_set, completed_hsh[:done]
  end

  def test_wait_first_exception # rubocop:disable Metrics/AbcSize
    future1 = AsyncFutures::Future.new
    future2 = AsyncFutures::Future.new
    future3 = AsyncFutures::Future.new

    all_fs = [future1, future2, future3]
    dup_fs = [future1, future2, future3, future1]

    all_fs.each(&:set_running_or_notify_cancel)

    any_exception = RuntimeError.new('whatever')

    future1.set_result(true)
    future2.set_exception(any_exception)

    completed_hsh = AsyncFutures::Future.wait(dup_fs, nil, AsyncFutures::Future::FIRST_EXCEPTION)

    assert_instance_of Hash, completed_hsh

    assert_includes completed_hsh, :done
    assert_includes completed_hsh, :not_done

    assert_instance_of Set, completed_hsh[:done]
    assert_instance_of Set, completed_hsh[:not_done]

    assert_equal 2, completed_hsh[:done].size
    assert_equal 1, completed_hsh[:not_done].size

    assert_equal [future1, future2].to_set, completed_hsh[:done]
  end

  def test_wait_first_exception_multiple # rubocop:disable Metrics/AbcSize
    future1 = AsyncFutures::Future.new
    future2 = AsyncFutures::Future.new
    future3 = AsyncFutures::Future.new

    all_fs = [future1, future2, future3]
    dup_fs = [future1, future2, future3, future1]

    all_fs.each(&:set_running_or_notify_cancel)

    any_exception = RuntimeError.new('whatever')

    future1.set_result(true)
    future2.set_exception(any_exception)
    future3.set_exception(any_exception)

    completed_hsh = AsyncFutures::Future.wait(dup_fs, nil, AsyncFutures::Future::FIRST_EXCEPTION)

    assert_instance_of Hash, completed_hsh

    assert_includes completed_hsh, :done
    assert_includes completed_hsh, :not_done

    assert_instance_of Set, completed_hsh[:done]
    assert_instance_of Set, completed_hsh[:not_done]

    assert_equal 3, completed_hsh[:done].size
    assert_equal 0, completed_hsh[:not_done].size

    assert_equal all_fs.to_set, completed_hsh[:done]
  end

  def test_as_completed # rubocop:disable Metrics/AbcSize
    future1 = AsyncFutures::Future.new
    future2 = AsyncFutures::Future.new
    future3 = AsyncFutures::Future.new

    all_fs = [future1, future2, future3]
    dup_fs = [future1, future2, future3, future1]

    all_fs.each(&:set_running_or_notify_cancel)
    all_fs.each_with_index { |f, i| f.set_result(i) }

    completed = AsyncFutures::Future.as_completed(dup_fs)

    completed_ary = completed.to_a

    refute_equal dup_fs.size, completed_ary.size
    assert_equal all_fs.size, completed_ary.size
    assert_equal all_fs.map(&:result), completed_ary.map(&:result)
  end

  def test_as_completed_multi_enumerate # rubocop:disable Metrics/AbcSize
    future1 = AsyncFutures::Future.new
    future2 = AsyncFutures::Future.new
    future3 = AsyncFutures::Future.new

    all_fs = [future1, future2, future3]
    dup_fs = [future1, future2, future3, future1]

    all_fs.each(&:set_running_or_notify_cancel)
    all_fs.each_with_index { |f, i| f.set_result(i) }

    completed = AsyncFutures::Future.as_completed(dup_fs)

    assert_instance_of Enumerator, completed

    completed_ary = completed.to_a

    assert_instance_of Array, completed_ary

    exc = assert_raises(RuntimeError) { completed.to_a }

    assert_match(/^Enumerator already consumed$/, exc.message)
  end

  def test_as_completed_follows_completion_order # rubocop:disable Metrics/AbcSize
    future1 = AsyncFutures::Future.new
    future2 = AsyncFutures::Future.new
    future3 = AsyncFutures::Future.new

    all_fs = [future1, future2, future3]
    dup_fs = [future1, future2, future3, future1]

    all_fs.each(&:set_running_or_notify_cancel)

    completed = AsyncFutures::Future.as_completed(dup_fs)

    all_fs.each_with_index.reverse_each { |f, i| f.set_result(i) }

    completed_ary = completed.to_a

    refute_equal all_fs.map(&:result), completed_ary.map(&:result)
    assert_equal all_fs.reverse_each.map(&:result), completed_ary.map(&:result)
  end

  def test_as_completed_with_timeout # rubocop:disable Metrics/AbcSize
    future1 = AsyncFutures::Future.new
    future2 = AsyncFutures::Future.new
    future3 = AsyncFutures::Future.new

    all_fs = [future1, future2, future3]
    dup_fs = [future1, future2, future3, future1]

    all_fs.each(&:set_running_or_notify_cancel)
    all_fs.each_with_index { |f, i| f.set_result(i) }

    completed = AsyncFutures::Future.as_completed(dup_fs, 0.1)

    completed_ary = completed.to_a

    refute_equal dup_fs.size, completed_ary.size
    assert_equal all_fs.size, completed_ary.size
    assert_equal all_fs.map(&:result), completed_ary.map(&:result)
  end

  def test_as_completed_with_failing_timeout
    future1 = AsyncFutures::Future.new
    future2 = AsyncFutures::Future.new
    future3 = AsyncFutures::Future.new

    all_fs = [future1, future2, future3]
    dup_fs = [future1, future2, future3, future1]

    all_fs.each(&:set_running_or_notify_cancel)
    all_fs.each_with_index { |f, i| f.set_result(i) }

    completed = AsyncFutures::Future.as_completed(dup_fs, 0.01)

    sleep 0.02

    assert_raises(Timeout::Error) { completed.to_a }
  end

  def test_as_completed_with_failing_timeout2
    future1 = AsyncFutures::Future.new
    future2 = AsyncFutures::Future.new
    future3 = AsyncFutures::Future.new

    all_fs = [future1, future2, future3]
    dup_fs = [future1, future2, future3, future1]

    all_fs.each(&:set_running_or_notify_cancel)
    all_fs.each_with_index { |f, i| f.set_result(i) }

    assert_raises(Timeout::Error) { AsyncFutures::Future.as_completed(dup_fs, -0.01) }
  end

  def test_as_completed_size
    future1 = AsyncFutures::Future.new
    future2 = AsyncFutures::Future.new
    future3 = AsyncFutures::Future.new

    all_fs = [future1, future2, future3]
    dup_fs = [future1, future2, future3, future1]

    all_fs.each(&:set_running_or_notify_cancel)
    all_fs.each_with_index { |f, i| f.set_result(i) }

    completed = AsyncFutures::Future.as_completed(dup_fs)

    assert_instance_of Enumerator, completed
    assert_equal all_fs.size, completed.size
  end

  def test_new_future_should_be_pending
    future1 = AsyncFutures::Future.new

    assert_predicate future1, :pending?
    refute_predicate future1, :running?
  end

  def test_complete_runs_on_pending_future
    future1 = AsyncFutures::Future.new

    assert_predicate future1, :pending?

    block_ran = false

    complete_result = future1.complete { block_ran = true }

    assert_same true, complete_result
    assert_same true, block_ran
  end

  def test_complete_doesnt_run_on_running_future
    future1 = AsyncFutures::Future.new

    assert_predicate future1, :pending?

    future1.set_running_or_notify_cancel

    assert_predicate future1, :running?

    block_ran = false

    complete_result = future1.complete { block_ran = true }

    assert_same false, complete_result
    assert_same false, block_ran
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
    assert_equal true, future1.cancel # rubocop:disable Minitest/AssertTruthy
    assert_predicate future1, :cancelled?
    assert_equal true, future1.cancel # rubocop:disable Minitest/AssertTruthy
    assert_predicate future1, :cancelled?
  end

  def test_calling_set_running_twice_logs_properly
    @mock = Minitest::Mock.new
    AsyncFutures.logger = @mock
    @mock.expect :unknown, nil

    future1 = AsyncFutures::Future.new
    future1.set_running_or_notify_cancel
    raised_exc = assert_raises(AsyncFutures::InvalidStateError) { future1.set_running_or_notify_cancel }
    assert_equal 'Future in unexpected state', raised_exc.message

    @mock.verify
  ensure
    AsyncFutures.logger = nil
  end

  def test_calling_set_running_twice_raises_exception
    future1 = AsyncFutures::Future.new
    future1.set_running_or_notify_cancel
    raised_exc = assert_raises(AsyncFutures::InvalidStateError) { future1.set_running_or_notify_cancel }
    assert_equal 'Future in unexpected state', raised_exc.message
  end

  def test_calling_set_running_after_cancel_returns_false
    future1 = AsyncFutures::Future.new
    future1.cancel
    value1 = future1.set_running_or_notify_cancel

    assert_equal false, value1 # rubocop:disable Minitest/RefuteFalse
  end

  def test_test_fiber_and_future_attrs
    future1 = AsyncFutures::Future.new

    assert_nil future1.fiber
    assert_nil future1.thread

    future1.fiber = Fiber.current
    future1.thread = Thread.current

    assert_same Fiber.current, future1.fiber
    assert_same Thread.current, future1.thread
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

    assert_same future1, future1.join(0.01)
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
