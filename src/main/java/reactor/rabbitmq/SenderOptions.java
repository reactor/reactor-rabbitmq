package reactor.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import reactor.core.scheduler.Scheduler;

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

    private Scheduler resourceCreationScheduler;

    private Scheduler connectionSubscriptionScheduler;

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

    /**
     * Resource creation scheduler.
     * It is developer's responsibility to close it if set.
     * @param resourceCreationScheduler
     * @return
     */
    public SenderOptions resourceCreationScheduler(Scheduler resourceCreationScheduler) {
        this.resourceCreationScheduler = resourceCreationScheduler;
        return this;
    }

    public Scheduler getConnectionSubscriptionScheduler() {
        return connectionSubscriptionScheduler;
    }

    /**
     * Scheduler used on connection creation subscription.
     * It is developer's responsibility to close it if set.
     * @param connectionSubscriptionScheduler
     * @return
     */
    public SenderOptions connectionSubscriptionScheduler(Scheduler connectionSubscriptionScheduler) {
        this.connectionSubscriptionScheduler = connectionSubscriptionScheduler;
        return this;
    }
}
