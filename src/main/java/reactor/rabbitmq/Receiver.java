/*
 * Copyright (c) 2017 Pivotal Software Inc, All Rights Reserved.
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
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 *
 */
public class Receiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

    private static final Function<Connection, Channel> CHANNEL_CREATION_FUNCTION = new Receiver.ChannelCreationFunction();

    private final Mono<Connection> connectionMono;

    private final AtomicBoolean hasConnection = new AtomicBoolean(false);

    public Receiver() {
        this(() -> {
           ConnectionFactory connectionFactory = new ConnectionFactory();
           connectionFactory.useNio();
           return connectionFactory;
        });
    }

    public Receiver(Supplier<ConnectionFactory> connectionFactorySupplier) {
        this(connectionFactorySupplier.get());
    }

    public Receiver(Supplier<ConnectionFactory> connectionFactorySupplier, Scheduler connectionSubscriptionScheduler) {
        this(connectionFactorySupplier.get(), connectionSubscriptionScheduler);
    }

    public Receiver(ConnectionFactory connectionFactory) {
        this(connectionFactory, Schedulers.parallel());
    }

    public Receiver(ConnectionFactory connectionFactory, Scheduler connectionSubscriptionScheduler) {
        this.connectionMono = Mono.fromCallable(() -> {
            Connection connection = connectionFactory.newConnection();
            return connection;
        }).doOnSubscribe(c -> hasConnection.set(true))
          .subscribeOn(connectionSubscriptionScheduler)
          .cache();
    }

    // TODO more consumeNoAck functions:
    //  - with a Supplier<Boolean> or Predicate<FluxSink> or Predicate<Delivery> to complete the Flux

    public Flux<Delivery> consumeNoAck(final String queue) {
        return consumeNoAck(queue, new ReceiverOptions().overflowStrategy(FluxSink.OverflowStrategy.IGNORE));
    }

    public Flux<Delivery> consumeNoAck(final String queue, ReceiverOptions options) {
        // TODO track flux so it can be disposed when the sender is closed?
        // could be also developer responsibility
        return Flux.create(emitter -> connectionMono.map(CHANNEL_CREATION_FUNCTION).subscribe(channel -> {
            try {
                DeliverCallback deliverCallback = (consumerTag, message) -> {
                    emitter.next(message);
                    if (options.getStopConsumingBiFunction().apply(emitter, message)) {
                        emitter.complete();
                    }
                };
                CancelCallback cancelCallback = consumerTag -> {
                    LOGGER.info("Flux consumer {} has been cancelled", consumerTag);
                    emitter.complete();
                };

                final String consumerTag = channel.basicConsume(queue, true, deliverCallback, cancelCallback);
                LOGGER.info("Consumer {} consuming from {} has been registered", consumerTag, queue);
                emitter.onDispose(() -> {
                    LOGGER.info("Cancelling consumer {} consuming from {}", consumerTag, queue);
                    try {
                        if(channel.isOpen() && channel.getConnection().isOpen()) {
                            channel.basicCancel(consumerTag);
                            channel.close();
                        }
                    } catch (TimeoutException | IOException e) {
                        throw new ReactorRabbitMqException(e);
                    }
                });
            } catch (IOException e) {
                throw new ReactorRabbitMqException(e);
            }
        }), options.getOverflowStrategy());
    }

    public Flux<Delivery> consumeAutoAck(final String queue) {
        return consumeAutoAck(queue, new ReceiverOptions().overflowStrategy(FluxSink.OverflowStrategy.BUFFER));
    }

    public Flux<Delivery> consumeAutoAck(final String queue, ReceiverOptions options) {
        // TODO why acking here and not just after emitter.next()?
        return consumeManuelAck(queue, options).doOnNext(msg -> msg.ack()).map(ackableMsg -> (Delivery) ackableMsg);
    }

    public Flux<AcknowledgableDelivery> consumeManuelAck(final String queue) {
        return consumeManuelAck(queue, new ReceiverOptions().overflowStrategy(FluxSink.OverflowStrategy.BUFFER));
    }

    public Flux<AcknowledgableDelivery> consumeManuelAck(final String queue, ReceiverOptions options) {
        // TODO track flux so it can be disposed when the sender is closed?
        // could be also developer responsibility
        return Flux.create(emitter -> connectionMono.map(CHANNEL_CREATION_FUNCTION).subscribe(channel -> {
            try {
                if(options.getQos() != 0) {
                    channel.basicQos(options.getQos());
                }

                DeliverCallback deliverCallback = (consumerTag, message) -> {
                    AcknowledgableDelivery delivery = new AcknowledgableDelivery(message, channel);
                    if(options.getHookBeforeEmit().apply(emitter, delivery)) {
                        emitter.next(delivery);
                    }
                    if (options.getStopConsumingBiFunction().apply(emitter, message)) {
                        emitter.complete();
                    }
                };
                CancelCallback cancelCallback = consumerTag -> {
                    LOGGER.info("Flux consumer {} has been cancelled", consumerTag);
                    emitter.complete();
                };

                final String consumerTag = channel.basicConsume(queue, false, deliverCallback, cancelCallback);
                emitter.onDispose(() -> {

                    try {
                        if (channel.isOpen() && channel.getConnection().isOpen()) {
                            channel.basicCancel(consumerTag);
                            channel.close();
                        }
                    } catch (TimeoutException | IOException e) {
                        throw new ReactorRabbitMqException(e);
                    }
                });
            } catch (IOException e) {
                throw new ReactorRabbitMqException(e);
            }
        }), options.getOverflowStrategy());
    }

    // TODO consume with dynamic QoS and/or batch ack

    public void close() {
        if (hasConnection.getAndSet(false)) {
            try {
                // FIXME use timeout on block (should be a parameter of the Receiver)
                connectionMono.block().close();
            } catch (IOException e) {
                throw new ReactorRabbitMqException(e);
            }
        }
    }

    private static class ChannelCreationFunction implements Function<Connection, Channel> {

        @Override
        public Channel apply(Connection connection) {
            try {
                return connection.createChannel();
            } catch (IOException e) {
                throw new ReactorRabbitMqException("Error while creating channel", e);
            }
        }
    }

}
