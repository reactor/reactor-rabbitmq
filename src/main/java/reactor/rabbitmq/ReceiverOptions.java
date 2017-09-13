package reactor.rabbitmq;

import reactor.core.publisher.FluxSink;

import java.util.function.BiFunction;

/**
 *
 */
public class ReceiverOptions {

    private int qos = 0;

    private FluxSink.OverflowStrategy overflowStrategy = FluxSink.OverflowStrategy.BUFFER;

    private BiFunction<FluxSink<?>, AcknowledgableDelivery, Boolean> hookBeforeEmit = (fluxSink, message) -> true;

    private BiFunction<FluxSink<?>, Delivery, Boolean> stopConsumingBiFunction = (fluxSink, message) -> false;

    public int getQos() {
        return qos;
    }

    public ReceiverOptions qos(int qos) {
        if(qos < 0) {
            throw new IllegalArgumentException("QoS must greater or equal to 0");
        }
        this.qos = qos;
        return this;
    }

    public FluxSink.OverflowStrategy getOverflowStrategy() {
        return overflowStrategy;
    }

    public ReceiverOptions overflowStrategy(FluxSink.OverflowStrategy overflowStrategy) {
        this.overflowStrategy = overflowStrategy;
        return this;
    }

    public BiFunction<FluxSink<?>, AcknowledgableDelivery, Boolean> getHookBeforeEmit() {
        return hookBeforeEmit;
    }

    public ReceiverOptions hookBeforeEmit(BiFunction<FluxSink<?>, AcknowledgableDelivery, Boolean> hookBeforeEmit) {
        this.hookBeforeEmit = hookBeforeEmit;
        return this;
    }

    public BiFunction<FluxSink<?>, Delivery, Boolean> getStopConsumingBiFunction() {
        return stopConsumingBiFunction;
    }

    public ReceiverOptions stopConsumingBiFunction(
        BiFunction<FluxSink<?>, Delivery, Boolean> stopConsumingBiFunction) {
        this.stopConsumingBiFunction = stopConsumingBiFunction;
        return this;
    }
}
