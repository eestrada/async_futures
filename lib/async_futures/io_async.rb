# frozen_string_literal: true

require_relative 'future'

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
    # The string is written in a nonblocking fashion
    # on a background worker thread.
    #
    # If the process shuts down before the future can be completed,
    # the work will be abandoned even if it partially completed.
    def write_async(string) # rubocop:disable Metrics/AbcSize
      Future.new.tap do |ftr|
        maybe_spawn_worker

        io_async_queue.push(proc {
          # :nocov:
          break ftr unless ftr.set_running_or_notify_cancel(set_context: true)
          # :nocov:

          all_written = 0

          loop do
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
        })
      end
    end

    # Return an incomplete future
    # that will eventually contain the string value read from the IO object
    # or an exception if the IO object could not be read from for some reason.
    #
    # The string is read in a nonblocking fashion
    # on a background worker thread.
    #
    # If the process shuts down before the future can be completed,
    # the work will be abandoned even if it partially completed.
    def read_async(maxlen) # rubocop:disable Metrics/AbcSize
      Future.new.tap do |ftr|
        maybe_spawn_worker

        io_async_queue.push(proc {
          # :nocov:
          break ftr unless ftr.set_running_or_notify_cancel(set_context: true)
          # :nocov:

          to_read_length = maxlen
          retrieved_str = String.new

          loop do
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
        })
      end
    end

    private

    def maybe_spawn_worker
      Ractor[:io_async_worker] ||= Thread.new do
        worker_fibers = Set.new
        loop do
          fproc = io_async_queue.pop(timeout: 0.001)

          worker_fibers.add(Fiber.new(&fproc)) unless fproc.nil?

          worker_fibers.reject!(&:resume) unless worker_fibers.empty?
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
    # This exists for compatibility
    # with classes with true nonblocking reading methods.
    # There is no performance benefit
    # to calling this instead of directly calling `write`.
    def write_async(string)
      Future.new.tap do |ftr|
        ftr.complete(string, &method(:write))
      end
    end

    # Return a completed future
    # containing a string up to `maxlen` bytes long.
    #
    # This exists for compatibility
    # with classes with true nonblocking reading methods.
    # There is no performance benefit
    # to calling this instead of directly calling `read`.
    def read_async(maxlen = nil)
      Future.new.tap do |ftr|
        ftr.complete(maxlen, &method(:read))
      end
    end
  end
end

IO.include AsyncFutures::IOAsync

OpenSSL::SSL::SSLSocket.include AsyncFutures::IOAsync

StringIO.include AsyncFutures::IOSync
