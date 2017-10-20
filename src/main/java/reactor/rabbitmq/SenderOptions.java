package reactor.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.function.Supplier;

/**
 * Options for {@link Sender} creation.
 */
public class SenderOptions {

    private ConnectionFactory connectionFactory = ((Supplier<ConnectionFactory>) () -> {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        return connectionFactory;
    }).get();

    private Scheduler resourceCreationScheduler = Schedulers.elastic();

    private Scheduler connectionSubscriptionScheduler = Schedulers.elastic();

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public SenderOptions connectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        return this;
    }

    public Scheduler getResourceCreationScheduler() {
        return resourceCreationScheduler;
    }

    public SenderOptions resourceCreationScheduler(Scheduler resourceCreationScheduler) {
        this.resourceCreationScheduler = resourceCreationScheduler;
        return this;
    }

    public Scheduler getConnectionSubscriptionScheduler() {
        return connectionSubscriptionScheduler;
    }

    public SenderOptions connectionSubscriptionScheduler(Scheduler connectionSubscriptionScheduler) {
        this.connectionSubscriptionScheduler = connectionSubscriptionScheduler;
        return this;
    }
}
