# frozen_string_literal: true

require_relative 'future'

require 'timeout'
require 'openssl'
require 'set' # rubocop:disable Lint/RedundantRequireStatement

module AsyncFutures
  # Simple example mixin for async IO.
  #
  # All reads and writes are done on a single background worker thread.
  #
  # This is not the most efficient implementation.
  # It is just meant to be an example
  # of how one can use the `Future` class
  # outside of an `Executor` implementation.
  module IOAsync
    # Return an incomplete future
    # that will eventually contain an integer with the number of bytes written
    # or an exception if the string could not be written for some reason.
    #
    # The `string` argument is written in a nonblocking fashion
    # on a background worker thread.
    #
    # The optional `timeout` argument
    # causes the work to finish the future exceptionally with `Timeout::Error`
    # if it takes longer than `timeout` seconds to complete.
    # This is used to avoid having background work that spins forever
    # on IO that may never complete.
    # If `nil` or no value is given, this means no timeout
    # (i.e. potentially spin indefinitely).
    #
    # This should *not* be confused with the `Timeout::Error` raised via the
    # `timeout` argument on `Future.result` and `Future.exception`.
    # If it matters for your purposes to differentiate between the two,
    # you can do something like the following:
    #
    # ```ruby
    # # `join` returns `nil` on timeout
    # result = if future.join(1.0)
    #            # If `Timeout::Error` is raised here,
    #            # it is from the `timeout` parameter to the `*_async` method.
    #            future.result
    #          else
    #            raise Timeout::Error.new('Timed out on `join`')
    #          end
    # ```
    #
    # The optional `sleep_timeout` keyword argument
    # is used to determine how quickly the worker thread
    # stops polling the input work queue
    # and how much sleep time happens between failed nonblocking IO attempts.
    # It defaults to 1ms.
    # If existing worker(s) have already been spawned,
    # then this argument isn't used.
    #
    # If the process shuts down before the future can be fully completed,
    # the work may be abandoned even if it partially completed.
    def write_async(string, timeout = nil, sleep_timeout: 0.001) # rubocop:disable Metrics/AbcSize,Metrics/CyclomaticComplexity
      Future.new.tap do |ftr|
        clock_timeout = timeout && (Time.now.to_f + timeout)

        work_proc = proc do
          # :nocov:
          break ftr unless ftr.set_running_or_notify_cancel(set_context: true)
          # :nocov:

          to_err = Timeout::Error.new('execution expired')
          all_written = 0

          loop do
            cur_to = timeout && (Time.now.to_f - clock_timeout)
            break ftr.tap { ftr.set_exception(to_err) } unless timeout.nil? || cur_to.positive?

            bytes_written = write_nonblock(string)
            string = string[bytes_written..nil]
            all_written += bytes_written
            break ftr.tap { ftr.set_result(all_written) } if string.empty?
          rescue IO::WaitReadable, IO::WaitWritable, Errno::EINTR
            Fiber.yield nil
            retry
          rescue Exception => e # rubocop:disable Lint/RescueException
            break ftr.tap { ftr.set_exception(e) }
          end
        end

        io_async_queue.push(work_proc)
        maybe_spawn_worker(sleep_timeout)
      end
    end

    # Return an incomplete future
    # that will eventually contain the string value read from the IO object
    # or an exception if the IO object could not be read from for some reason.
    #
    # A string up to `maxlen` in length is read in a nonblocking fashion
    # on a background worker thread.
    #
    # The optional `timeout` argument
    # causes the work to finish the future exceptionally with `Timeout::Error`
    # if it takes longer than `timeout` seconds to complete.
    # This is used to avoid having background work that spins forever
    # on IO that may never complete.
    # If `nil` or no value is given, this means no timeout
    # (i.e. potentially spin indefinitely).
    #
    # This should *not* be confused with the `Timeout::Error` raised via the
    # `timeout` argument on `Future.result` and `Future.exception`.
    # If it matters for your purposes to differentiate between the two,
    # you can do something like the following:
    #
    # ```ruby
    # # `join` returns `nil` on timeout
    # result = if future.join(1.0)
    #            # If `Timeout::Error` is raised here,
    #            # it is from the `timeout` parameter to the `*_async` method.
    #            future.result
    #          else
    #            raise Timeout::Error.new('Timed out on `join`')
    #          end
    # ```
    #
    # The optional `sleep_timeout` keyword argument
    # is used to determine how quickly the worker thread
    # stops polling the input work queue
    # and how much sleep time happens between failed nonblocking IO attempts.
    # It defaults to 1ms.
    # If existing worker(s) have already been spawned,
    # then this argument isn't used.
    #
    # If the process shuts down before the future can be fully completed,
    # the work may be abandoned even if it partially completed.
    def read_async(maxlen, timeout = nil, sleep_timeout: 0.001) # rubocop:disable Metrics/AbcSize,Metrics/CyclomaticComplexity
      Future.new.tap do |ftr|
        clock_timeout = timeout && (Time.now.to_f + timeout)

        work_proc = proc do
          # :nocov:
          break ftr unless ftr.set_running_or_notify_cancel(set_context: true)
          # :nocov:

          to_err = Timeout::Error.new('execution expired')
          to_read_length = maxlen
          retrieved_str = String.new

          loop do
            cur_to = timeout && (Time.now.to_f - clock_timeout)
            break ftr.tap { ftr.set_exception(to_err) } unless timeout.nil? || cur_to.positive?

            retrieved_str << read_nonblock(to_read_length)
            to_read_length = maxlen - retrieved_str.size
            break ftr.tap { ftr.set_result(retrieved_str) } if to_read_length.zero?
          rescue IO::WaitReadable, IO::WaitWritable, Errno::EINTR
            Fiber.yield nil
            retry
          rescue EOFError
            break ftr.tap { ftr.set_result(retrieved_str) }
          rescue Exception => e # rubocop:disable Lint/RescueException
            break ftr.tap { ftr.set_exception(e) }
          end
        end

        io_async_queue.push(work_proc)
        maybe_spawn_worker(sleep_timeout)
      end
    end

    private

    # This method will spawn *at least* one worker thread.
    # It *may* spawn more than one worker thread
    # based on submission thread timing,
    # but that is ok,
    # because they will all eventually reap once they run out of work
    # and the individual work is grabbed exclusively per thread
    # via the thread safe queue.
    #
    # If work starts up again after reaping all threads,
    # then new worker thread(s) will be spawned again.
    def maybe_spawn_worker(sleep_timeout)
      Ractor[:io_async_worker] ||= Thread.new do
        worker_fibers = Set.new

        while (fproc = io_async_queue.pop(timeout: sleep_timeout))
          worker_fibers.add(Fiber.new(&fproc))
          worker_fibers.reject!(&:resume)
        end

        # We need to unset the worker thread
        # since we're no longer polling the input queue.
        #
        # If more work comes in,
        # new worker thread(s) need to be spawned.
        Ractor[:io_async_worker] = nil

        # FIXME: if we have IO that never finishes,
        # this will create zombie threads that busy loop and never complete.
        until worker_fibers.empty?
          # Sleep when no fiber in the set completes
          sleep(sleep_timeout) unless worker_fibers.reject!(&:resume)
        end
      end
    end

    def io_async_queue
      Ractor.store_if_absent(:io_async_queue) { Thread::Queue.new }
    end
  end

  # Simple mixin for sync IO with an async interface.
  module IOSync
    # Return a completed future
    # containing an integer with the number of bytes written.
    #
    # This exists for classes such as `StringIO` to maintain compatibility
    # with classes with true nonblocking reading methods (such as `IO`).
    #
    # There is no performance benefit
    # to calling this instead of directly calling `write`.
    # In fact,
    # there may be a slight performance degradation
    # because of the added overhead of instantiating
    # and completing a `Future` object.
    #
    # You should only use this method if you are dealing with a mix
    # of `IO`, `OpenSSL::SSLSocket`, and/or `StringIO` objects
    # and want to interact with them identically in a nonblocking manner.
    # Or you may want to use this method with `StringIO`
    # as a type of mock object for testing
    # in place of real `IO` or `OpenSSL::SSLSocket` objects.
    def write_async(string, *args, **kwargs) # rubocop:disable Lint/UnusedMethodArgument
      Future.new.tap do |ftr|
        ftr.complete(string, &method(:write))
      end
    end

    # Return a completed future
    # containing a string up to `maxlen` bytes long.
    #
    # This exists for classes such as `StringIO` to maintain compatibility
    # with classes with true nonblocking reading methods (such as `IO`).
    #
    # There is no performance benefit
    # to calling this instead of directly calling `read`.
    # In fact,
    # there may be a slight performance degradation
    # because of the added overhead of instantiating
    # and completing a `Future` object.
    #
    # You should only use this method if you are dealing with a mix
    # of `IO`, `OpenSSL::SSLSocket`, and/or `StringIO` objects
    # and want to interact with them identically in a nonblocking manner.
    # Or you may want to use this method with `StringIO`
    # as a type of mock object for testing
    # in place of real `IO` or `OpenSSL::SSLSocket` objects.
    def read_async(maxlen, *args, **kwargs) # rubocop:disable Lint/UnusedMethodArgument
      Future.new.tap do |ftr|
        ftr.complete(maxlen, &method(:read))
      end
    end
  end
end

IO.include AsyncFutures::IOAsync

OpenSSL::SSL::SSLSocket.include AsyncFutures::IOAsync

StringIO.include AsyncFutures::IOSync
