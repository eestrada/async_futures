# frozen_string_literal: true

require_relative 'minitest_helper'

require 'async'
require 'async_futures/fiber_executor'

class TestFiberExecutor < Minitest::Test # rubocop:disable Metrics/ClassLength
  def setup
    @scheduler = Async::Scheduler.new
    Fiber.set_scheduler @scheduler
    @executor = AsyncFutures::FiberExecutor.new
  end

  def teardown
    @executor&.shutdown(wait: true, cancel_futures: false)
    Fiber.set_scheduler nil
  end

  def test_initialize_raises_without_scheduler_set
    Fiber.set_scheduler nil
    exc = assert_raises(AsyncFutures::Error) { AsyncFutures::FiberExecutor.new }

    assert_match(/No Fiber.scheduler set/, exc.message)
  end

  def test_submit_raises_argument_error_without_block
    assert_raises(ArgumentError) { @executor.submit('No block given') }
  end

  def test_submit_returns_a_future_object
    Fiber.schedule do
      AsyncFutures::FiberExecutor.new.shutdown do |executor|
        future1 = executor.submit(1, 2, 3, 4, tell_me: 'that you love me more') do |*args, **kwargs|
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
    end
  end

  def test_submit_raises_returns_exceptional_future
    Fiber.schedule do
      AsyncFutures::FiberExecutor.new.shutdown do |executor|
        future1 = executor.submit(1, 2, 3, 4, tell_me: 'that you love me more') do |*args, **kwargs|
          raise "Some runtime error #{args} #{kwargs}"
        end

        assert_instance_of AsyncFutures::Future, future1
        refute_nil future1.exception
        assert_predicate future1, :done?
      end
    end
  end

  def test_submit_concurrent_is_not_alias
    refute_equal @executor.method(:submit), @executor.method(:submit_concurrent)
  end

  def test_map
    Fiber.schedule do
      AsyncFutures::FiberExecutor.new.shutdown do |executor|
        enum = [1, 2, 3, 4]
        map_result = executor.map(enum, &:to_s)

        assert_instance_of Enumerator::Lazy, map_result

        results = map_result.to_a
        first = results.to_a[0]
        last = results.to_a[3]

        assert_instance_of String, first
        assert_equal '1', first
        assert_equal '4', last
      end
    end
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

    assert_match(/FiberExecutor instance is shutdown/, exc.message)
  end

  # This might not trigger if other threads are running.
  #
  # This prints some errors that can maybe be silenced.
  # Or maybe the printout needs to be addressed.
  # Just fix it somehow.
  def test_non_blocking_submit_with_blocking_shutdown
    Fiber.schedule do
      AsyncFutures::FiberExecutor.new.shutdown do |executor|
        executor.submit { sleep(0.02) }

        Fiber.blocking do
          exc = assert_raises(Exception) { executor.shutdown(wait: true) }

          assert_match(/No live threads left. Deadlock\?/, exc.message)
        end
      end
    end
  end

  def test_cancel_futures_in_shutdown
    Fiber.schedule do
      AsyncFutures::FiberExecutor.new.shutdown do |executor|
        future1 = executor.submit { sleep(0.02) }
        future1.join
        future2 = executor.submit { sleep(0.02) }
        future3 = executor.submit { sleep(0.02) }

        executor.shutdown(cancel_futures: true)

        refute_predicate future1, :cancelled?

        assert_predicate future2, :done?

        # Because the FiberExecutor immediately runs tasks in a non-blocking
        # Fiber, it is effectively impossible that they can be canceled before starting.
        assert_predicate future3, :done?
      end
    end
  end

  def test_cancel_futures_manually # rubocop:disable Metrics/AbcSize
    Fiber.schedule do
      AsyncFutures::FiberExecutor.new.shutdown do |executor|
        future1 = executor.submit { sleep(0.02) }
        future2 = executor.submit { sleep(0.02) }
        future3 = executor.submit { sleep(0.02) }

        assert_predicate future2, :running?
        assert_predicate future3, :running?

        future2.cancel
        future3.cancel
        future1.result

        # Fiber futures always run immediately,
        # so it is impossible to cancel them after starting them.
        refute_predicate future1, :cancelled?
        refute_predicate future2, :cancelled?
        refute_predicate future3, :cancelled?
      end
    end
  end
end
