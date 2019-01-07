package reactor.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import reactor.core.publisher.Flux;

import java.util.UUID;

public class SenderBenchmarkUtils {

    public static Flux<OutboundMessage> outboundMessageFlux(String queue, int nbMessages) {
        return Flux.range(0, nbMessages).map(i -> new OutboundMessage("", queue, "".getBytes()));
    }

    public static Connection newConnection() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        return connectionFactory.newConnection();
    }

    public static String declareQueue(Connection connection) throws Exception {
        String queueName = UUID.randomUUID().toString();
        Channel channel = connection.createChannel();
        String queue = channel.queueDeclare(queueName, false, false, false, null).getQueue();
        channel.close();
        return queue;
    }

    public static void deleteQueue(Connection connection, String queue) throws Exception {
        Channel channel = connection.createChannel();
        channel.queueDelete(queue);
        channel.close();
    }

}
