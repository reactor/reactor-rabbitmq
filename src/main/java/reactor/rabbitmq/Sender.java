package reactor.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class Sender {

    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

    private final Mono<Connection> connectionMono;

    public Sender() {
        this.connectionMono = Mono.fromCallable(() -> {
            // TODO provide connection settings
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.useNio();
            // TODO handle exception
            Connection connection = connectionFactory.newConnection();
            return connection;
        }).cache();
    }

    // TODO return Mono<Void> to know when the sending is done
    public void send(Publisher<OutboundMessage> messages) {
        Flux<OutboundMessage> flux = Flux.from(messages);
        final Mono<Channel> channelMono = Mono.fromCallable(() -> connectionMono.block().createChannel()).cache();
        flux.subscribe(message -> {
            try {
                channelMono.block().basicPublish(
                    message.getExchange(),
                    message.getRoutingKey(),
                    message.getProperties(),
                    message.getBody()
                );
            } catch(IOException e) {
                throw new ReactorRabbitMqException(e);
            }
        }, exception -> {
            LOGGER.warn("Send failed with exception {}", exception);
        }, () -> {
            try {
                channelMono.block().close();
            } catch (TimeoutException | IOException e) {
                throw new ReactorRabbitMqException(e);
            }
        });
    }

    public void close() {
        try {
            connectionMono.block().close();
        } catch (IOException e) {
            throw new ReactorRabbitMqException(e);
        }
    }

}
