require_relative 'frame'

module Nsq
  class Message < Frame

    attr_reader :attempts
    attr_reader :id
    attr_reader :body

    def initialize(data, connection)
      super
      ts_1, ts_2, @attempts, @id, @body = @data.unpack('NNs>a16a*')
      @timestamp_in_nanoseconds = (ts_1 * (2**32)) + ts_2
      @body.force_encoding('UTF-8')
    end

    def finish
      connection.fin(id)
    end

    def requeue(timeout = 0)
      connection.req(id, timeout)
    end

    def touch
      connection.touch(id)
    end

    def timestamp
      Time.at(@timestamp_in_nanoseconds / 1_000_000_000.0)
    end

  end
end
