# frozen_string_literal: true

require_relative 'minitest_helper'

require 'async_futures/thread_executor'

class TestThreadExecutor < Minitest::Test # rubocop:disable Metrics/ClassLength
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

  def test_submit_after_shutdown
    @executor.shutdown

    exc = assert_raises(RuntimeError) { @executor.submit { 'hello' } }

    assert_match(/ThreadExecutor instance is shutdown/, exc.message)
  end

  def test_set_thread_prefix
    new_executor = AsyncFutures::ThreadExecutor.new(thread_name_prefix: 'best')

    future1 = new_executor.submit { Thread.current.name }

    result = future1.result

    assert_match(/^best_\d+$/, result)
  end

  def test_only_one_worker # rubocop:disable Metrics/AbcSize
    new_executor = AsyncFutures::ThreadExecutor.new(max_workers: 1)

    before_count = Thread.list.size
    future1 = new_executor.submit { sleep(0.01) }
    future2 = new_executor.submit { sleep(0.01) }
    future3 = new_executor.submit { sleep(0.01) }
    future1.result
    future2.result
    future3.result
    after_count = Thread.list.size

    assert_operator after_count, :>, before_count
    assert_equal before_count + 1, after_count
  ensure
    new_executor.shutdown
  end

  def test_cancel_futures_in_shutdown
    new_executor = AsyncFutures::ThreadExecutor.new(max_workers: 1)

    future1 = new_executor.submit { sleep(0.01) }
    future1.result
    future2 = new_executor.submit { sleep(0.01) }
    future3 = new_executor.submit { sleep(0.01) }

    new_executor.shutdown(cancel_futures: true)

    refute_predicate future1, :cancelled?

    # Based on scheduling race conditions,
    # future2 could be cancelled or not.
    # We don't know when the worker thread will take control
    # and pick up another task.
    # However, because there is only one worker thread,
    # we know it can't pick up the third submitted task
    # while it is "working" on the second,
    # so assuming that the machine running this test isn't dog slow,
    # we should be able to cancel future3 before the sleep runs out.
    #
    # Thus why we only check that future2 is "done?".
    # We don't know for certain it will be canceled
    # or if the worker thread will pick it up.
    assert_predicate future2, :done?

    assert_predicate future3, :cancelled?
  ensure
    new_executor.shutdown
  end

  def test_cancel_futures_manually
    AsyncFutures::ThreadExecutor.new(max_workers: 1).shutdown do |new_executor|
      future1 = new_executor.submit { sleep(0.01) }
      future2 = new_executor.submit { sleep(0.01) }
      future3 = new_executor.submit { sleep(0.01) }

      assert_predicate future2, :pending?
      assert_predicate future3, :pending?

      future2.cancel
      future3.cancel
      future1.result

      refute_predicate future1, :cancelled?
      assert_predicate future2, :cancelled?
      assert_predicate future3, :cancelled?
    end
  end

  def test_reap_after # rubocop:disable Metrics/AbcSize
    AsyncFutures::ThreadExecutor.new(max_workers: 1, reap_after: 0.05).shutdown do |new_executor|
      count1 = Thread.list.size
      future1 = new_executor.submit { sleep(0.01) }
      future2 = new_executor.submit { sleep(0.01) }
      future3 = new_executor.submit { sleep(0.01) }
      future1.result
      future2.result
      future3.result
      count2 = Thread.list.size
      # sleep should cause worker thread to self-reap
      sleep 0.1
      count3 = Thread.list.size

      assert_operator count2, :>, count1
      assert_operator count2, :>, count3
    end
  end

  def test_no_reap_after # rubocop:disable Metrics/AbcSize
    AsyncFutures::ThreadExecutor.new(max_workers: 1, reap_after: nil).shutdown do |new_executor|
      count1 = Thread.list.size
      future1 = new_executor.submit { sleep(0.01) }
      future2 = new_executor.submit { sleep(0.01) }
      future3 = new_executor.submit { sleep(0.01) }
      future1.result
      future2.result
      future3.result
      count2 = Thread.list.size
      # worker thread will never self-reap
      sleep 0.1
      count3 = Thread.list.size

      assert_operator count2, :>, count1
      refute_operator count2, :>, count3
      assert_equal count2, count3
    end
  end
end
