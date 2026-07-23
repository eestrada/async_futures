# frozen_string_literal: true

require_relative 'minitest_helper'

class TestProcessExecutor < Minitest::Test # rubocop:disable Metrics/ClassLength
  def setup
    # No `Process.fork` available in these engines
    if RUBY_ENGINE =~ /jruby/ || RUBY_ENGINE =~ /truffleruby/
      skip "process_executor not supported in engine '#{RUBY_ENGINE}'"
    end

    require 'async_futures/process_executor'
    require 'logger'

    @executor = AsyncFutures::ProcessExecutor.new

    @sleep_mult = case RUBY_ENGINE
                  when /jruby/, /truffleruby/
                    4
                  else
                    1
                  end
    # AsyncFutures.logger = Logger.new($stderr)
    AsyncFutures.logger = Logger.new($stderr)
  end

  def teardown
    @executor&.shutdown(wait: true)
    AsyncFutures.logger = nil
  end

  def test_submit_raises_argument_error_without_block
    assert_raises(ArgumentError) { @executor.submit('No block given') }
  end

  def test_submit_returns_a_future_object
    AsyncFutures::ProcessExecutor.new.shutdown do |executor|
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

  def test_submit_raises_returns_exceptional_future
    future1 = @executor.submit(1, 2, 3, 4, tell_me: 'that you love me more') do |*args, **kwargs|
      raise "Some runtime error #{args} #{kwargs}"
    end

    assert_instance_of AsyncFutures::Future, future1
    refute_nil future1.exception

    assert_instance_of RuntimeError, future1.exception
    assert_predicate future1, :done?
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

    assert_match(/ProcessExecutor instance is shutdown/, exc.message)
  end

  def test_set_worker_name_prefix
    AsyncFutures::ProcessExecutor.new(worker_name_prefix: 'best').shutdown do |executor|
      future1 = executor.submit { AsyncFutures.worker_name }

      result = future1.result

      assert_match(/^best_\d+$/, result)

      refute_match AsyncFutures.worker_name, result
    end
  end

  def test_worker_no_daemonize
    AsyncFutures::ProcessExecutor.new(daemonize_workers: false).shutdown do |executor|
      future1 = executor.submit { Process.ppid }

      # Should NOT be child of init process.
      assert_equal Process.pid, future1.result
    end
  end

  def test_worker_daemonize
    AsyncFutures::ProcessExecutor.new(daemonize_workers: true).shutdown do |executor|
      future1 = executor.submit { Process.ppid }

      # Should be child of init process.
      refute_equal Process.pid, future1.result
    end
  end

  def test_terminate_workers # rubocop:disable Metrics/AbcSize
    AsyncFutures::ProcessExecutor.new(daemonize_workers: false).shutdown do |executor|
      before_time = Time.now.to_f
      future1 = executor.submit { sleep(1) }

      sleep(0.001) until future1.running?

      running_wait_time = Time.now.to_f - before_time

      assert_operator running_wait_time, :<, 1

      executor.terminate_workers
      future1.join

      skip 'This still sleeps the full time'

      wait_time = Time.now.to_f - before_time

      assert_operator wait_time, :<, 1
    end
  end

  def test_kill_workers # rubocop:disable Metrics/AbcSize
    AsyncFutures::ProcessExecutor.new(daemonize_workers: false).shutdown do |executor|
      before_time = Time.now.to_f
      future1 = executor.submit { sleep(1) }

      sleep(0.001) until future1.running?

      running_wait_time = Time.now.to_f - before_time

      assert_operator running_wait_time, :<, 1

      executor.kill_workers

      skip 'This still sleeps the full time'

      future1.join

      future1.result

      wait_time = Time.now.to_f - before_time

      assert_operator wait_time, :<, 1
    end
  end

  def test_only_one_worker # rubocop:disable Metrics/AbcSize
    skip "Timings aren't working right now"

    AsyncFutures::ProcessExecutor.new(max_workers: 1).shutdown do |executor|
      before_time = Time.now

      future1 = executor.submit(@sleep_mult) { |sleep_mult| sleep(0.02 * sleep_mult) }
      future2 = executor.submit(@sleep_mult) { |sleep_mult| sleep(0.02 * sleep_mult) }
      future3 = executor.submit(@sleep_mult) { |sleep_mult| sleep(0.02 * sleep_mult) }
      future1.result
      future2.result
      future3.result

      after_time = Time.now
      expected_wait = 0.02 * @sleep_mult * 3
      wait_difference = after_time.to_f - before_time.to_f

      assert_operator after_time, :>, before_time
      assert_operator expected_wait, :<=, wait_difference
    end
  end

  def test_cancel_futures_in_shutdown # rubocop:disable Metrics/AbcSize
    AsyncFutures::ProcessExecutor.new(max_workers: 1).shutdown do |executor|
      future1 = executor.submit { sleep(0.1) }
      future2 = executor.submit { sleep(0.1) }
      future3 = executor.submit { sleep(0.1) }

      # make sure first job gets picked up.
      Thread.pass

      executor.shutdown(wait: false, cancel_futures: true)

      future1.join

      refute_predicate future1, :cancelled?

      assert_instance_of Integer, future1.result
      refute_predicate future1.result, :negative?

      future2.join
      future3.join

      # Based on scheduling race conditions,
      # future2 could be cancelled or not.
      # We don't know when the worker will pick up another task.
      # However, because there is only one worker thread,
      # we know it can't pick up the third submitted task
      # while it is "working" on the second.
      #
      # Thus why we only check that future2 is "done?".
      # We don't know for certain it will be canceled
      # or if the worker thread will pick it up.
      assert_predicate future2, :done?

      assert_predicate future3, :cancelled?
    end
  end

  def test_dont_wait_in_shutdown
    AsyncFutures::ProcessExecutor.new(max_workers: 1).shutdown do |executor|
      future1 = executor.submit(@sleep_mult) { |sleep_mult| sleep(0.02 * sleep_mult) }
      future1.join
      executor.submit(@sleep_mult) { |sleep_mult| sleep(0.02 * sleep_mult) }
      future3 = executor.submit(@sleep_mult) { |sleep_mult| sleep(0.02 * sleep_mult) }

      executor.shutdown(cancel_futures: false, wait: false)

      assert_predicate future1, :done?

      refute_predicate future3, :done?
    end
  end

  def test_cancel_futures_manually # rubocop:disable Metrics/AbcSize
    AsyncFutures::ProcessExecutor.new(max_workers: 1).shutdown do |executor|
      future1 = executor.submit(@sleep_mult) { |sleep_mult| sleep(0.02 * sleep_mult) }
      future2 = executor.submit(@sleep_mult) { |sleep_mult| sleep(0.02 * sleep_mult) }
      future3 = executor.submit(@sleep_mult) { |sleep_mult| sleep(0.02 * sleep_mult) }

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
end
