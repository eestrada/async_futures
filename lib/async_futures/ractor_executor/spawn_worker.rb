# frozen_string_literal: true

module AsyncFutures
  class RactorExecutor # rubocop:disable Style/Documentation
    private

    # Always spawn a worker
    def spawn_worker # rubocop:disable Metrics/AbcSize
      new_port = Ractor::Port.new

      worker = Ractor.new(new_port, @move_result, name: new_worker_name) do |results_port, move_result|
        loop do
          case (task = Ractor.receive)
          when :shutdown
            break
          when Array
            future_id, block, args, kwargs = task
          else
            raise RactorError.new("Unknown message received: #{task}")
          end

          begin
            result = block.call(*args, **kwargs)
          rescue Exception => e # rubocop:disable Lint/RescueException
            results_port.send([future_id, :exception, e], move: move_result)
          else
            results_port.send([future_id, :result, result], move: move_result)
          end
        end
      end

      worker.tap do |worker|
        @work_ports[new_port] = worker
        worker.monitor new_port
        @pool.add worker
        @available_workers.push worker
      end
    end
  end
end
