# frozen_string_literal: true

require_relative 'minitest_helper'

require 'async_futures/thread_executor'

class TestThreadExecutor < Minitest::Test # rubocop:disable Metrics/ClassLength
  def setup
    @executor = AsyncFutures::ThreadExecutor.new

    @sleep_mult = case RUBY_ENGINE
                  when /jruby/, /truffleruby/
                    8
                  else
                    1
                  end
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
    before_count = @executor.pool_size
    future1 = @executor.submit(1, 2, 3, 4, tell_me: 'that you love me more') do |*args, **kwargs|
      raise "Some runtime error #{args} #{kwargs}"
    end
    after_count = @executor.pool_size

    assert_operator after_count, :>, before_count
    assert_instance_of AsyncFutures::Future, future1
    refute_nil future1.exception
    assert_predicate future1, :done?
  end

  def test_run_deadlocking_submission_on_closed_executor
    AsyncFutures::ThreadExecutor.new(max_workers: 1, strict_concurrency: true).shutdown do |executor|
      m1 = Thread::Mutex.new

      future1 = m1.synchronize do
        executor.submit(executor, m1) do |meta_exec, mtx|
          meta_exec.shutdown(wait: false)

          # should raise on shutdown
          mtx.synchronize { meta_exec.submit { 1234 } }
        end.tap do |f1| # rubocop:disable Style/MultilineBlockChain
          # The parent future should *not* be run immediately.
          refute_predicate f1, :done?
        end
      end

      # <RuntimeError: ThreadExecutor instance is shutdown>
      assert_instance_of RuntimeError, future1.exception
      assert_match(/^ThreadExecutor instance is shutdown$/, future1.exception.message)
    end
  end

  def test_run_deadlocking_submission_immediately
    AsyncFutures::ThreadExecutor.new(max_workers: 1, strict_concurrency: true).shutdown do |executor|
      m1 = Thread::Mutex.new

      future1 = m1.synchronize do
        executor.submit(executor, m1) do |meta_exec, mtx|
          f_inner1 = meta_exec.submit { 1234 }

          # Because the submission would deadlock,
          # it is run immediately
          # instead of being scheduled for later.
          assert_predicate f_inner1, :done?

          mtx.synchronize { f_inner1.result }
        end.tap do |f1| # rubocop:disable Style/MultilineBlockChain
          # The parent future should *not* be run immediately.
          refute_predicate f1, :done?
        end
      end

      assert_equal 1234, future1.result
    end
  end

  def test_run_deadlocking_submission_after_shutdown
    AsyncFutures::ThreadExecutor.new(max_workers: 1).shutdown do |executor|
      m1 = Thread::Mutex.new

      future1 = m1.synchronize do
        executor.submit(executor, m1) do |meta_exec, mtx|
          meta_exec.shutdown(wait: false)

          mtx.synchronize { nil }

          assert_raises(RuntimeError) { meta_exec.submit { 1234 } }
        end.tap do |f1| # rubocop:disable Style/MultilineBlockChain
          # The parent future should *not* be run immediately.
          refute_predicate f1, :done?
        end
      end

      exc = future1.result

      assert_match(/^ThreadExecutor instance is shutdown$/, exc.message)
    end
  end

  def test_concurrent_submission_success
    AsyncFutures::ThreadExecutor.new(max_workers: 1).shutdown do |executor|
      m1 = Thread::Mutex.new

      future1 = m1.synchronize do
        executor.submit_concurrent(m1) do |mtx|
          mtx.synchronize { 1234 }
        end.tap do |f1| # rubocop:disable Style/MultilineBlockChain
          # The concurrent future should *not* be completed.
          refute_predicate f1, :done?
        end
      end

      assert_equal 1234, future1.result
    end
  end

  def test_concurrent_submission_no_block
    AsyncFutures::ThreadExecutor.new(max_workers: 1).shutdown do |executor|
      exc = assert_raises(ArgumentError) { executor.submit_concurrent }

      assert_match(/^No block given$/, exc.message)
    end
  end

  def test_concurrent_submission_deadlocking
    AsyncFutures::ThreadExecutor.new(max_workers: 1, strict_concurrency: true).shutdown do |executor|
      m1 = Thread::Mutex.new

      future1 = m1.synchronize do
        executor.submit(executor, m1) do |meta_exec, mtx|
          mtx.synchronize do
            assert_raises(AsyncFutures::NoConcurrencyError) do
              meta_exec.submit_concurrent { 1234 }
            end
          end
        end.tap do |f1| # rubocop:disable Style/MultilineBlockChain
          # The future should *not* be completed.
          refute_predicate f1, :done?
        end
      end

      exc = future1.result

      assert_match(/^Tasks exceed potential workers$/, exc.message)
    end
  end

  def test_concurrent_submission_after_shutdown_single_worker
    AsyncFutures::ThreadExecutor.new(max_workers: 1).shutdown do |executor|
      m1 = Thread::Mutex.new

      future1 = m1.synchronize do
        executor.submit(executor, m1) do |meta_exec, mtx|
          mtx.synchronize do
            meta_exec.shutdown(wait: false)

            assert_raises(RuntimeError) do
              meta_exec.submit_concurrent { 1234 }
            end
          end
        end.tap do |f1| # rubocop:disable Style/MultilineBlockChain
          # The future should *not* be completed.
          refute_predicate f1, :done?
        end
      end

      exc = future1.result

      assert_match(/^ThreadExecutor instance is shutdown$/, exc.message)
    end
  end

  def test_concurrent_submission_after_shutdown_multi_worker
    AsyncFutures::ThreadExecutor.new(max_workers: 2).shutdown do |executor|
      m1 = Thread::Mutex.new

      future1 = m1.synchronize do
        executor.submit(executor, m1) do |meta_exec, mtx|
          mtx.synchronize do
            meta_exec.shutdown(wait: false)

            assert_raises(RuntimeError) do
              meta_exec.submit_concurrent { 1234 }
            end
          end
        end.tap do |f1| # rubocop:disable Style/MultilineBlockChain
          # The future should *not* be completed.
          refute_predicate f1, :done?
        end
      end

      exc = future1.result

      assert_match(/^ThreadExecutor instance is shutdown$/, exc.message)
    end
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

  def test_set_worker_name_prefix
    new_executor = AsyncFutures::ThreadExecutor.new(worker_name_prefix: 'best')

    future1 = new_executor.submit { Thread.current.name }

    result = future1.result

    assert_match(/^best_\d+$/, result)
  end

  def test_only_one_worker
    AsyncFutures::ThreadExecutor.new(max_workers: 1).shutdown do |executor|
      before_count = executor.pool_size
      future1 = executor.submit(1) { |n| n }
      future2 = executor.submit(2) { |n| n }
      future3 = executor.submit(3) { |n| n }
      future1.result
      future2.result
      future3.result
      after_count = executor.pool_size

      assert_operator after_count, :>, before_count
      assert_equal before_count + 1, after_count
    end
  end

  def test_cancel_futures_in_shutdown # rubocop:disable Metrics/AbcSize
    AsyncFutures::ThreadExecutor.new(max_workers: 1).shutdown do |executor|
      m1 = Thread::Mutex.new
      m2 = Thread::Mutex.new
      m3 = Thread::Mutex.new

      m1.lock
      m2.lock
      m3.lock

      future1 = executor.submit(m1) { |mtx| mtx.synchronize { 1 } }
      future2 = executor.submit(m2) { |mtx| mtx.synchronize { 2 } }
      future3 = executor.submit(m3) { |mtx| mtx.synchronize { 3 } }

      m1.unlock
      future1.join

      executor.shutdown(wait: false, cancel_futures: true)

      refute_predicate future1, :cancelled?

      assert_equal 1, future1.result

      m2.unlock
      m3.unlock

      future2.join
      future3.join

      # Based on scheduling race conditions,
      # future2 could be cancelled or not.
      # We don't know when the worker thread will take control
      # and pick up another task.
      # However, because there is only one worker thread,
      # we know it can't pick up the third submitted task
      # while it is "working" on the second.
      #
      # Thus why we only check that future2 is "done?".
      # We don't know for certain it will be canceled
      # or if the worker thread will pick it up.
      assert_predicate future2, :done?

      assert_predicate future3, :cancelled?
    ensure
      m1.unlock if m1.locked?
      m2.unlock if m2.locked?
      m3.unlock if m3.locked?
    end
  end

  def test_cancel_futures_manually # rubocop:disable Metrics/AbcSize
    AsyncFutures::ThreadExecutor.new(max_workers: 1).shutdown do |executor|
      m1 = Thread::Mutex.new

      m1.lock

      future1 = executor.submit(m1) { |mtx| mtx.synchronize { 1 } }
      future2 = executor.submit(m1) { |mtx| mtx.synchronize { 2 } }
      future3 = executor.submit(m1) { |mtx| mtx.synchronize { 3 } }

      assert_predicate future2, :pending?
      assert_predicate future3, :pending?

      future2.cancel
      future3.cancel

      m1.unlock

      future1.result

      refute_predicate future1, :cancelled?
      assert_predicate future2, :cancelled?
      assert_predicate future3, :cancelled?
    ensure
      m1.unlock if m1.locked?
    end
  end

  def test_reap_after # rubocop:disable Metrics/AbcSize
    AsyncFutures::ThreadExecutor.new(max_workers: 1, reap_after: 0.03 * @sleep_mult).shutdown do |executor|
      count1 = executor.pool_size
      future1 = executor.submit { 1 }
      future2 = executor.submit { 2 }
      future3 = executor.submit { 3 }
      future1.result
      future2.result
      future3.result
      count2 = executor.pool_size
      # sleep should cause worker thread to self-reap
      sleep 0.05 * @sleep_mult
      count3 = executor.pool_size

      assert_operator count2, :>, count1
      assert_operator count2, :>, count3
    end
  end

  def test_no_reap_after # rubocop:disable Metrics/AbcSize
    AsyncFutures::ThreadExecutor.new(max_workers: 1, reap_after: nil).shutdown do |executor|
      count1 = executor.pool_size
      future1 = executor.submit { 1 }
      future2 = executor.submit { 2 }
      future3 = executor.submit { 3 }
      future1.result
      future2.result
      future3.result
      count2 = executor.pool_size
      # worker thread will never self-reap
      sleep 0.05 * @sleep_mult
      count3 = executor.pool_size

      assert_operator count2, :>, count1
      refute_operator count2, :>, count3
      assert_equal count2, count3
    end
  end
end
