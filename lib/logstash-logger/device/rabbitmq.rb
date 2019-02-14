require 'bunny'

module LogStashLogger
  module Device
    class RabbitMq < Connectable

      DEFAULT_PORT = 56721
      DEFAULT_EXCHANGE_NAME = 'logstash'
      DEFAULT_ROUTING_KEY = 'logstashkey'

      attr_accessor :port, :exchange_name, :routing_key

      def initialize(opts)
        super
        @port = opts[:port] || DEFAULT_PORT
        @exchange_name = opts[:exchange_name] || DEFAULT_EXCHANGE_NAME
        @routing_key = opts[:routing_key] || DEFAULT_ROUTING_KEY
      end
  
      def connect
        @conn = Bunny.new(:port => @port)
        @conn.start
        channel = @conn.create_channel
        raise 'exchange doesnt exist' unless @conn.exchange_exists? @exchange_name
        @exchange = channel.direct(@exchange_name, :durable => true)
        @io = @conn
      end

      def write_one(message)
        @exchange.publish message, :routing_key => @routing_key
      end

      def close!
        @conn.close
      end
    end
  end
end
