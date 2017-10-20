package reactor.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.function.Supplier;

/**
 * Options for {@link Receiver} creation.
 */
public class ReceiverOptions {

    private ConnectionFactory connectionFactory = ((Supplier<ConnectionFactory>) () -> {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        return connectionFactory;
    }).get();

    private Scheduler connectionSubscriptionScheduler = Schedulers.elastic();

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public ReceiverOptions connectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        return this;
    }

    public Scheduler getConnectionSubscriptionScheduler() {
        return connectionSubscriptionScheduler;
    }

    public ReceiverOptions connectionSubscriptionScheduler(Scheduler connectionSubscriptionScheduler) {
        this.connectionSubscriptionScheduler = connectionSubscriptionScheduler;
        return this;
    }
}
