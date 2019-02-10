require 'bunny'

module LogStashLogger
  module Device
    class RabbitMq < Connectable

      DEFAULT_ROUTING_KEY = 'logstashkey'

      def initialize(opts)
        super
      end

      def connect
        @conn = Bunny.new
        @conn.start
        raise 'exchange doesnt exist' unless conn.exchange_exists?("logstash")
        channel = conn.create_channel
        @exchange = channel.direct("logstash", :durable => true)
        @io = @conn
      end

      def write_one(message)
        @exchange.publish message :routing_key => DEFAULT_ROUTING_KEY
      end

      def close!
        @conn.close
      end
    end
  end
end
