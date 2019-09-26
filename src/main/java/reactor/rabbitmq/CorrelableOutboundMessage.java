package reactor.rabbitmq;

import java.util.Arrays;

import com.rabbitmq.client.AMQP;

/**
 * An outbound message meant to be sent by a {@link Sender} and may contain (unserialized)
 * correlated metadata
 */
public class CorrelableOutboundMessage<T> extends OutboundMessage {

    private final T correlationMetadata;

    /**
     * Constructs a new message which is described by the body, the target exchange and the routing key which
     * can be used for smart routing after the message is published to the exchange.
     *
     * @param exchange            The name of the target exchange.
     * @param routingKey          The routing key to be used if the message has to be routed in a specific way towards a queue.
     * @param body                The main body of the message.
     * @param correlationMetadata The (unserialized) metadata correlated with this Message.
     */
    public CorrelableOutboundMessage(String exchange, String routingKey, byte[] body, T correlationMetadata) {
        super(exchange, routingKey, body);
        this.correlationMetadata = correlationMetadata;
    }

    /**
     * Constructs a new message which is described by the body, the target exchange and the routing key which
     * can be used for smart routing after the message is published to the exchange. The message will also be
     * accompanied by the provided properties which define its behaviour in the broker.
     *
     * @param exchange            The name of the target exchange.
     * @param routingKey          The routing key to be used if the message has to be routed in a specific way towards a queue.
     * @param properties          AMQP compatible properties that will be used during the publishing of the message.
     * @param body                The main body of the message.
     * @param correlationMetadata The (unserialized) metadata correlated with this Message.
     */
    public CorrelableOutboundMessage(String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body, T correlationMetadata) {
        super(exchange, routingKey, properties, body);
        this.correlationMetadata = correlationMetadata;
    }

    /**
     * Defines the "correlation" metadata associated with a sent OutboundMessage
     *
     * @return The correlated metadata of the message
     */
    public T getCorrelationMetadata() {
        return correlationMetadata;
    }

    @Override
    public String toString() {
        return "CorrelableOutboundMessage{" +
            "exchange='" + getExchange() + '\'' +
            ", routingKey='" + getRoutingKey() + '\'' +
            ", properties=" + getProperties() + '\'' +
            ", body=" + Arrays.toString(getBody()) + '\'' +
            ", correlationMetadata=" + getCorrelationMetadata() +
            '}';
    }
}
