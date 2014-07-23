require 'celluloid/io'

module Nsq
  class Connection

    include Celluloid::IO

    attr_reader :socket

    def initialize(host, port)
      @socket = Celluloid::IO::TCPSocket.new(host, port)
      write '  V2'
    end

    trap_exit :actor_died
    def actor_died(actor, reason)
      p "Oh no! #{actor.inspect} has died because of a #{reason.class}"
    end

    def listen_for_messages(queue)
      loop do
        frame = receive_frame
        if frame.is_a?(Response)
          puts "response: #{frame.data}"
        elsif frame.is_a?(Error)
          puts "error: #{frame.data}"
        elsif frame.is_a?(Message)
          queue.push(frame)
        end
      end
    end


    def sub(topic, channel)
      write "SUB #{topic} #{channel}\n"
    end


    def rdy(count)
      write "RDY #{count}\n"
    end


    def fin(message_id)
      write "FIN #{message_id}\n"
    end


    def req(message_id)
      write "REQ #{message_id}\n"
    end


    def touch(message_id)
      write "TOUCH #{message_id}\n"
    end


    def cls
      write "CLS\n"
    end


    def nop
      write "NOP\n"
    end


    def pub(topic, message)
      write ["PUB #{topic}\n", message.length, message].pack('a*l>a*')
    end


    def mpub(topic, messages)
      body = messages.map do |message|
        [message.length, message].pack('l>a*')
      end.join

      write ["MPUB #{topic}\n", body.length, messages.size, body].pack('a*l>l>a*')
    end


    private
    def write(raw)
      @socket.write(raw)
    end


    def receive_frame
      if buffer = @socket.read(8)
        size, type = buffer.unpack('l>l>')
        size -= 4 # we want the size of the data part and type already took up 4 bytes
        data = @socket.read(size)
        Frame.build(type, data, self)
      end
    end

  end
end