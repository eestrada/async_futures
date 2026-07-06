# frozen_string_literal: true

require_relative 'minitest_helper'

class TestRactorExecutor < Minitest::Test # rubocop:disable Metrics/ClassLength
  def setup
    # skip 'Keep coverage pristine for now'

    # The Ractor API was different before version 4.x of Ruby.
    skip "ractor_executor not supported in version '#{RUBY_VERSION}'" if RUBY_VERSION =~ /^3\./
    if RUBY_ENGINE =~ /jruby/ || RUBY_ENGINE =~ /truffleruby/
      skip "ractor_executor not supported in engine '#{RUBY_ENGINE}'"
    end

    require 'async_futures/ractor_executor'
    require 'logger'

    @executor = AsyncFutures::RactorExecutor.new

    @sleep_mult = case RUBY_ENGINE
                  when /jruby/, /truffleruby/
                    4
                  else
                    1
                  end
    # AsyncFutures.logger = Logger.new($stderr)
    AsyncFutures.logger = nil
  end

  def teardown
    @executor&.shutdown(wait: true)
    AsyncFutures.logger = nil
  end

  def test_new_with_conflicting_args
    exc = assert_raises(ArgumentError) do
      AsyncFutures::RactorExecutor.new(copy_args: true, make_args_shareable: false)
    end

    assert_match(/^`copy_args` cannot be true unless `make_args_shareable` is also true$/, exc.message)
  end

  def test_shareable_args
    AsyncFutures::RactorExecutor.new(copy_args: true, make_args_shareable: true).shutdown do |executor|
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

  def test_submit_raises_argument_error_without_block
    assert_raises(ArgumentError) { @executor.submit('No block given') }
  end

  def test_submit_returns_a_future_object
    AsyncFutures::RactorExecutor.new.shutdown do |executor|
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
    before_count = Ractor.count
    future1 = @executor.submit(1, 2, 3, 4, tell_me: 'that you love me more') do |*args, **kwargs|
      raise "Some runtime error #{args} #{kwargs}"
    end
    after_count = Ractor.count

    assert_operator after_count, :>, before_count
    assert_instance_of AsyncFutures::Future, future1
    refute_nil future1.exception
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

    assert_match(/RactorExecutor instance is shutdown/, exc.message)
  end

  def test_set_worker_name_prefix
    AsyncFutures::RactorExecutor.new(worker_name_prefix: 'best').shutdown do |executor|
      future1 = executor.submit { Ractor.current.name }

      result = future1.result

      assert_match(/^best_\d+$/, result)
    end
  end

  def test_exceptional_exit
    AsyncFutures::RactorExecutor.new(max_workers: 2).shutdown do |executor|
      future1 = executor.submit { Ractor.current.default_port }

      default_port1 = future1.result

      # Under normal circumstances
      # we can't cause an abnormal exit from a Ractor worker,
      # so we need to force one here by writing directly to its default port.
      default_port1.send 'Nonsense value'

      future2 = executor.submit { Ractor.current.default_port }

      skip 'With only one worker, I cannot get this to work correctly'

      # FIXME: This should be caught and dealt with before this future is even spawned.
      # This only fails if there is only one worker.
      assert_raises(AsyncFutures::RactorError) { future2.result }

      default_port2 = future2.result

      refute_same default_port1, default_port2
    end
  end

  def test_only_one_worker # rubocop:disable Metrics/AbcSize
    AsyncFutures::RactorExecutor.new(max_workers: 1).shutdown do |executor|
      before_count = Ractor.count
      future1 = executor.submit(@sleep_mult) { |sleep_mult| sleep(0.02 * sleep_mult) }
      future2 = executor.submit(@sleep_mult) { |sleep_mult| sleep(0.02 * sleep_mult) }
      future3 = executor.submit(@sleep_mult) { |sleep_mult| sleep(0.02 * sleep_mult) }
      future1.result
      future2.result
      future3.result
      after_count = Ractor.count

      assert_operator after_count, :>, before_count
      assert_equal before_count + 1, after_count
    end
  end

  def test_cancel_futures_in_shutdown # rubocop:disable Metrics/AbcSize
    AsyncFutures::RactorExecutor.new(max_workers: 1).shutdown do |executor|
      future1 = executor.submit(@sleep_mult) { |sleep_mult| sleep(0.02 & sleep_mult) }
      future1.join
      future2 = executor.submit(@sleep_mult) { |sleep_mult| sleep(0.02 & sleep_mult) }
      future3 = executor.submit(@sleep_mult) { |sleep_mult| sleep(0.02 & sleep_mult) }

      executor.shutdown(cancel_futures: true)

      refute_predicate future1, :cancelled?

      # Based on scheduling race conditions,
      # future2 could be cancelled or not.
      # We don't know when the worker will pick up another task.
      # However, because there is only one worker,
      # we know it can't pick up the third submitted task
      # while it is "working" on the second,
      # so assuming that the machine running this test isn't dog slow,
      # we should be able to cancel future3 before the sleep runs out.
      #
      # Thus why we only check that future2 is "done?".
      # We don't know for certain it will be canceled
      # or if the worker will pick it up.
      assert_predicate future2, :done?

      assert_predicate future3, :cancelled?
    end
  end

  def test_dont_wait_in_shutdown
    AsyncFutures::RactorExecutor.new(max_workers: 1).shutdown do |executor|
      future1 = executor.submit(@sleep_mult) { |sleep_mult| sleep(0.02 & sleep_mult) }
      future1.join
      executor.submit(@sleep_mult) { |sleep_mult| sleep(0.02 & sleep_mult) }
      future3 = executor.submit(@sleep_mult) { |sleep_mult| sleep(0.02 & sleep_mult) }

      executor.shutdown(cancel_futures: false, wait: false)

      assert_predicate future1, :done?

      refute_predicate future3, :done?
    end
  end

  def test_cancel_futures_manually # rubocop:disable Metrics/AbcSize
    AsyncFutures::RactorExecutor.new(max_workers: 1).shutdown do |executor|
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

  def test_reap_after # rubocop:disable Metrics/AbcSize
    skip 'skip everywhere for now'
    AsyncFutures::RactorExecutor.new(max_workers: 1, reap_after: 0.03 * @sleep_mult).shutdown do |new_executor|
      count1 = Thread.list.size
      future1 = new_executor.submit { sleep(0.01 * @sleep_mult) }
      future2 = new_executor.submit { sleep(0.01 * @sleep_mult) }
      future3 = new_executor.submit { sleep(0.01 * @sleep_mult) }
      future1.result
      future2.result
      future3.result
      count2 = Thread.list.size
      # sleep should cause worker thread to self-reap
      sleep 0.05 * @sleep_mult
      count3 = Thread.list.size

      assert_operator count2, :>, count1
      assert_operator count2, :>, count3
    end
  end

  def test_no_reap_after # rubocop:disable Metrics/AbcSize
    skip 'skip everywhere for now'
    AsyncFutures::RactorExecutor.new(max_workers: 1, reap_after: nil).shutdown do |new_executor|
      count1 = Thread.list.size
      future1 = new_executor.submit { sleep(0.01 * @sleep_mult) }
      future2 = new_executor.submit { sleep(0.01 * @sleep_mult) }
      future3 = new_executor.submit { sleep(0.01 * @sleep_mult) }
      future1.result
      future2.result
      future3.result
      count2 = Thread.list.size
      # worker thread will never self-reap
      sleep 0.05 * @sleep_mult
      count3 = Thread.list.size

      assert_operator count2, :>, count1
      refute_operator count2, :>, count3
      assert_equal count2, count3
    end
  end
end
