package reactor.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import reactor.util.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Delayed outbound message meant to be sent by a {@link Sender}.
 * The <a href="https://github.com/rabbitmq/rabbitmq-delayed-message-exchange">delayed message plugin</a>
 * need to be enabled for RabbitMQ.
 */
public class DelayedOutboundMessage extends OutboundMessage {

    public static final String DELAYED_HEADER_KEY = "x-delay";

    /**
     * @param delayMillis Delay time in milliseconds
     */
    public DelayedOutboundMessage(String exchange, String routingKey, long delayMillis, byte[] body) {
        super(exchange, routingKey, delayedProperties(null, delayMillis), body);
    }

    /**
     * @param delayMillis Delay time in milliseconds
     */
    public DelayedOutboundMessage(String exchange, String routingKey, long delayMillis, BasicProperties properties, byte[] body) {
        super(exchange, routingKey, delayedProperties(properties, delayMillis), body);
    }

    private static BasicProperties delayedProperties(@Nullable AMQP.BasicProperties source, long delayMillis) {
        AMQP.BasicProperties.Builder builder;
        if (source != null) {
            builder = source.builder();
        } else {
            builder = new BasicProperties.Builder();
        }
        Map<String, Object> headers;
        if (source != null) {
            headers = source.getHeaders();
        } else {
            headers = new HashMap<>();
        }
        headers.put(DELAYED_HEADER_KEY, delayMillis);
        return builder.headers(headers).build();
    }

}
