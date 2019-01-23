/*
 * Copyright (c) 2017-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.rabbitmq;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Reactive abstraction to consume messages as a {@link Flux}.
 */
public class Receiver implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

    private static final Function<Connection, Channel> CHANNEL_CREATION_FUNCTION = new Receiver.ChannelCreationFunction();

    private final Mono<? extends Connection> connectionMono;

    private final AtomicBoolean hasConnection = new AtomicBoolean(false);

    private final Scheduler connectionSubscriptionScheduler;

    private final boolean privateConnectionSubscriptionScheduler;

    public Receiver() {
        this(new ReceiverOptions());
    }

    public Receiver(ReceiverOptions options) {
        this.privateConnectionSubscriptionScheduler = options.getConnectionSubscriptionScheduler() == null;
        this.connectionSubscriptionScheduler = options.getConnectionSubscriptionScheduler() == null ?
            createScheduler("rabbitmq-receiver-connection-subscription") : options.getConnectionSubscriptionScheduler();
        this.connectionMono = options.getConnectionMono() != null ? options.getConnectionMono() :
            Mono.fromCallable(() -> options.getConnectionFactory().newConnection())
                .doOnSubscribe(c -> hasConnection.set(true))
                .subscribeOn(this.connectionSubscriptionScheduler)
                .cache();
    }

    protected Scheduler createScheduler(String name) {
        return Schedulers.newElastic(name);
    }

    // TODO more consumeNoAck functions:
    //  - with a Supplier<Boolean> or Predicate<FluxSink> or Predicate<Delivery> to complete the Flux

    public Flux<Delivery> consumeNoAck(final String queue) {
        return consumeNoAck(queue, new ConsumeOptions());
    }

    public Flux<Delivery> consumeNoAck(final String queue, ConsumeOptions options) {
        return Flux.create(emitter -> connectionMono.map(CHANNEL_CREATION_FUNCTION).subscribe(channel -> {
            try {
                DeliverCallback deliverCallback = (consumerTag, message) -> {
                    emitter.next(message);
                    if (options.getStopConsumingBiFunction().apply(emitter.requestedFromDownstream(), message)) {
                        emitter.complete();
                    }
                };
                CancelCallback cancelCallback = consumerTag -> {
                    LOGGER.info("Flux consumer {} has been cancelled", consumerTag);
                    emitter.complete();
                };

                completeOnChannelShutdown(channel, emitter);

                final String consumerTag = channel.basicConsume(queue, true, deliverCallback, cancelCallback);
                AtomicBoolean cancelled = new AtomicBoolean(false);
                LOGGER.info("Consumer {} consuming from {} has been registered", consumerTag, queue);
                emitter.onDispose(() -> {
                    LOGGER.info("Cancelling consumer {} consuming from {}", consumerTag, queue);
                    if (cancelled.compareAndSet(false, true)) {
                        try {
                            if (channel.isOpen() && channel.getConnection().isOpen()) {
                                channel.basicCancel(consumerTag);
                                channel.close();
                            }
                        } catch (TimeoutException | IOException e) {
                            // Not sure what to do, not much we can do,
                            // logging should be enough.
                            // Maybe one good reason to introduce an exception handler to choose more easily.
                            LOGGER.warn("Error while closing channel: " + e.getMessage());
                        }
                    }
                });
            } catch (IOException e) {
                throw new RabbitFluxException(e);
            }
        }, emitter::error), options.getOverflowStrategy());
    }

    protected void completeOnChannelShutdown(Channel channel, FluxSink<?> emitter) {
        channel.addShutdownListener(reason -> {
            if (!AutorecoveringConnection.DEFAULT_CONNECTION_RECOVERY_TRIGGERING_CONDITION.test(reason)) {
                emitter.complete();
            }
        });
    }

    public Flux<Delivery> consumeAutoAck(final String queue) {
        return consumeAutoAck(queue, new ConsumeOptions());
    }

    public Flux<Delivery> consumeAutoAck(final String queue, ConsumeOptions options) {
        // TODO why acking here and not just after emitter.next()?
        return consumeManualAck(queue, options)
                .doOnNext(AcknowledgableDelivery::ack)
                .map(ackableMsg -> ackableMsg);
    }

    public Flux<AcknowledgableDelivery> consumeManualAck(final String queue) {
        return consumeManualAck(queue, new ConsumeOptions());
    }

    public Flux<AcknowledgableDelivery> consumeManualAck(final String queue, ConsumeOptions options) {
        // TODO track flux so it can be disposed when the sender is closed?
        // could be also developer responsibility
        return Flux.create(emitter -> connectionMono.map(CHANNEL_CREATION_FUNCTION).subscribe(channel -> {
            try {
                if (options.getQos() != 0) {
                    channel.basicQos(options.getQos());
                }

                DeliverCallback deliverCallback = (consumerTag, message) -> {
                    AcknowledgableDelivery delivery = new AcknowledgableDelivery(message, channel, options.getExceptionHandler());
                    if (options.getHookBeforeEmitBiFunction().apply(emitter.requestedFromDownstream(), delivery)) {
                        emitter.next(delivery);
                    }
                    if (options.getStopConsumingBiFunction().apply(emitter.requestedFromDownstream(), message)) {
                        emitter.complete();
                    }
                };
                CancelCallback cancelCallback = consumerTag -> {
                    LOGGER.info("Flux consumer {} has been cancelled", consumerTag);
                    emitter.complete();
                };

                completeOnChannelShutdown(channel, emitter);

                final String consumerTag = channel.basicConsume(queue, false, deliverCallback, cancelCallback);
                AtomicBoolean cancelled = new AtomicBoolean(false);
                emitter.onDispose(() -> {
                    LOGGER.info("Cancelling consumer {} consuming from {}", consumerTag, queue);
                    if (cancelled.compareAndSet(false, true)) {
                        try {
                            if (channel.isOpen() && channel.getConnection().isOpen()) {
                                channel.basicCancel(consumerTag);
                                channel.close();
                            }
                        } catch (TimeoutException | IOException e) {
                            // Not sure what to do, not much we can do,
                            // logging should be enough.
                            // Maybe one good reason to introduce an exception handler to choose more easily.
                            LOGGER.warn("Error while closing channel: " + e.getMessage());
                        }
                    }
                });
            } catch (IOException e) {
                throw new RabbitFluxException(e);
            }
        }, emitter::error), options.getOverflowStrategy());
    }

    // TODO consume with dynamic QoS and/or batch ack

    public void close() {
        if (hasConnection.getAndSet(false)) {
            try {
                // FIXME use timeout on block (should be a parameter of the Receiver)
                connectionMono.block().close();
            } catch (IOException e) {
                throw new RabbitFluxException(e);
            }
        }
        if (privateConnectionSubscriptionScheduler) {
            this.connectionSubscriptionScheduler.dispose();
        }
    }

    public static class AcknowledgmentContext {

        private final AcknowledgableDelivery delivery;
        private final Consumer<AcknowledgableDelivery> consumer;

        public AcknowledgmentContext(AcknowledgableDelivery delivery, Consumer<AcknowledgableDelivery> consumer) {
            this.delivery = delivery;
            this.consumer = consumer;
        }

        public void ackOrNack() {
            consumer.accept(delivery);
        }
    }

    private static class ChannelCreationFunction implements Function<Connection, Channel> {

        @Override
        public Channel apply(Connection connection) {
            try {
                return connection.createChannel();
            } catch (IOException e) {
                throw new RabbitFluxException("Error while creating channel", e);
            }
        }
    }
}
