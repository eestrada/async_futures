# frozen_string_literal: true

require_relative 'minitest_helper'

require 'tempfile'
require 'minitest/mock'

class TestIOAsync < Minitest::Test # rubocop:disable Metrics/ClassLength
  def setup
    skip "IOAsync not supported in engine '#{RUBY_ENGINE}'" if RUBY_ENGINE =~ /jruby/ || RUBY_ENGINE =~ /truffleruby/

    require 'async_futures/io_async'
  end

  def test_stringio
    str = String.new('hello')
    sio = StringIO.new(str)
    sio.seek(0, IO::SEEK_END)
    f1 = sio.write_async(' world')

    assert_instance_of AsyncFutures::Future, f1
    assert_predicate f1, :done?
    assert_equal 6, f1.result
    assert_equal 'hello world', sio.string

    sio.rewind
    f2 = sio.read_async(7)

    assert_instance_of AsyncFutures::Future, f2
    assert_predicate f2, :done?
    assert_equal 'hello w', f2.result
  end

  def test_io_async_read_to_eof
    Tempfile.create do |tf|
      tf.write 'foo'
      tf.rewind
      f1 = tf.read_async(20)

      assert_instance_of AsyncFutures::Future, f1

      result = f1.result(1)

      assert_equal 'foo', result

      # Should not be run on main thread
      refute_same Thread.current, f1.thread
    end
  end

  def test_io_async_read_partial
    Tempfile.create do |tf|
      tf.write 'foo'
      tf.rewind
      f1 = tf.read_async(2)

      assert_instance_of AsyncFutures::Future, f1

      result = f1.result(1)

      assert_equal 'fo', result

      # Should not be run on main thread
      refute_same Thread.current, f1.thread
    end
  end

  def test_io_async_read_raise_waitreadable # rubocop:disable Metrics/AbcSize
    Tempfile.create do |tf|
      mtx = Thread::Mutex.new
      tf.write 'foo'
      tf.rewind

      mtx.lock

      test_error = StandardError.new

      test_error.extend(IO::WaitReadable)

      waitreadable_proc = proc do
        raise test_error if mtx.locked?

        'fo'
      end

      tf.stub(:read_nonblock, waitreadable_proc) do
        f1 = tf.read_async(2)

        assert_instance_of AsyncFutures::Future, f1

        assert_raises(Timeout::Error) { f1.result(0.05) }

        mtx.unlock
        result = f1.result(1)

        assert_equal 'fo', result

        # Should not be run on main thread
        refute_same Thread.current, f1.thread
      end
    ensure
      mtx.unlock if mtx.locked?
    end
  end

  def test_io_async_read_raise_timeout_exception
    Tempfile.create do |tf|
      mtx = Thread::Mutex.new
      mtx.lock

      waitreadable_proc = proc do
        mtx.synchronize { 2 }
      end

      tf.write 'foo'
      tf.rewind

      tf.stub(:read_nonblock, waitreadable_proc) do
        f1 = tf.read_async(2, 0.01)

        f1.join(0.05)
        mtx.unlock
        result = f1.exception(1)

        assert_instance_of Timeout::Error, result
      end
    ensure
      mtx.unlock if mtx.locked?
    end
  end

  def test_io_async_read_raise_exception
    Tempfile.create do |tf|
      tf.write 'foo'
      tf.rewind

      test_error = StandardError.new('Test exception')

      waitreadable_proc = proc do
        raise test_error
      end

      tf.stub(:read_nonblock, waitreadable_proc) do
        f1 = tf.read_async(2)

        assert_instance_of AsyncFutures::Future, f1

        exc = assert_raises(StandardError) { f1.result(0.05) }

        assert_match(/Test exception/, exc.message)
      end
    end
  end

  def test_io_async_write_to_eof
    Tempfile.create do |tf|
      f1 = tf.write_async('foo')

      assert_instance_of AsyncFutures::Future, f1

      result = f1.result(1)

      assert_equal 3, result

      # Should not be run on main thread
      refute_same Thread.current, f1.thread
    end
  end

  def test_io_async_write_partial # rubocop:disable Metrics/AbcSize
    Tempfile.create do |tf|
      mtx = Thread::Mutex.new
      mtx.lock

      waitwritable_proc = proc do
        if mtx.locked?
          mtx.synchronize { 2 }
        else
          1
        end
      end

      tf.stub(:write_nonblock, waitwritable_proc) do
        f1 = tf.write_async('foo')

        assert_instance_of AsyncFutures::Future, f1

        assert_raises(Timeout::Error) { f1.result(0.05) }
        mtx.unlock

        result = f1.result(1)

        assert_equal 3, result

        # Should not be run on main thread
        refute_same Thread.current, f1.thread
      end
    ensure
      mtx.unlock if mtx.locked?
    end
  end

  def test_io_async_write_raise_waitwritable # rubocop:disable Metrics/AbcSize
    Tempfile.create do |tf|
      mtx = Thread::Mutex.new
      mtx.lock

      test_error = StandardError.new
      test_error.extend(IO::WaitWritable)
      waitwritable_proc = proc do
        raise test_error if mtx.locked?

        2
      end

      tf.stub(:write_nonblock, waitwritable_proc) do
        f1 = tf.write_async('fo')

        assert_instance_of AsyncFutures::Future, f1
        assert_raises(Timeout::Error) { f1.result(0.05) }
        mtx.unlock
        result = f1.result(1)

        assert_equal 2, result

        # Should not be run on main thread
        refute_same Thread.current, f1.thread
      end
    ensure
      mtx.unlock if mtx.locked?
    end
  end

  def test_io_async_write_raise_timeout_error # rubocop:disable Metrics/AbcSize
    Tempfile.create do |tf|
      mtx = Thread::Mutex.new
      mtx.lock

      waitwritable_proc = proc do
        mtx.synchronize { 2 }
      end

      tf.stub(:write_nonblock, waitwritable_proc) do
        f1 = tf.write_async('fo', 0.03)

        assert_instance_of AsyncFutures::Future, f1

        f1.join(0.05)
        mtx.unlock
        result = f1.exception(1)

        assert_instance_of Timeout::Error, result

        # Should not be run on main thread
        refute_same Thread.current, f1.thread
      end
    ensure
      mtx.unlock if mtx.locked?
    end
  end

  def test_io_async_write_raise_exception
    Tempfile.create do |tf|
      test_error = StandardError.new('Test exception')

      write_nonblock_proc = proc do
        raise test_error
      end

      tf.stub(:write_nonblock, write_nonblock_proc) do
        f1 = tf.write_async('foo')

        assert_instance_of AsyncFutures::Future, f1

        exc = assert_raises(StandardError) { f1.result(0.05) }

        assert_match(/Test exception/, exc.message)
      end
    end
  end
end
