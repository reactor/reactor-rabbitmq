package reactor.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

import java.io.IOException;

/**
 *
 */
public class AcknowledgableDelivery extends Delivery {

    private final Channel channel;

    public AcknowledgableDelivery(Envelope envelope, AMQP.BasicProperties properties, byte[] body, Channel channel) {
        super(envelope, properties, body);
        this.channel = channel;
    }

    public void ack() {
        try {
            channel.basicAck(getEnvelope().getDeliveryTag(), false);
        } catch (IOException e) {
            throw new ReactorRabbitMqException(e);
        }
    }
}
