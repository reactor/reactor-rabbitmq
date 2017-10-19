package reactor.rabbitmq;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 *
 */
public class SenderOptions {

    private Scheduler resourceCreationScheduler = Schedulers.elastic();

    private Scheduler connectionSubscriptionScheduler = Schedulers.elastic();

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
