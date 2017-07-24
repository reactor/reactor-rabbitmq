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

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 *
 */
public class Receiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

    private final Mono<Connection> connectionMono;

    // using specific scheduler to avoid being cancelled in subscribe
    // see https://github.com/reactor/reactor-core/issues/442
    private final Scheduler scheduler = Schedulers.fromExecutor(
        Executors.newFixedThreadPool(Schedulers.DEFAULT_POOL_SIZE),
        true
    );

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

    public Receiver(ConnectionFactory connectionFactory) {
        this.connectionMono = Mono.fromCallable(() -> {
            Connection connection = connectionFactory.newConnection();
            return connection;
        }).subscribeOn(scheduler)
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
        return Flux.create(emitter -> {
            connectionMono.subscribe(connection -> {
                try {
                    // TODO handle exception
                    Channel channel = connection.createChannel();
                    final DefaultConsumer consumer = new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                            emitter.next(new Delivery(envelope, properties, body));
                        }

                        @Override
                        public void handleCancel(String consumerTag) throws IOException {
                            LOGGER.info("Flux consumer {} has been cancelled", consumerTag);
                        }
                    };
                    final String consumerTag = channel.basicConsume(queue, true, consumer);
                    emitter.onDispose(() -> {
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
            });

        }, options.getOverflowStrategy());
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
        return Flux.create(emitter -> {
            connectionMono.subscribe(connection -> {
                try {

                    Channel channel = connection.createChannel();
                    if(options.getQos() != 0) {
                        channel.basicQos(options.getQos());
                    }
                    final DefaultConsumer consumer = new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                            AcknowledgableDelivery message = new AcknowledgableDelivery(envelope, properties, body, getChannel());
                            if(options.getHookBeforeEmit().apply(emitter, message)) {
                                emitter.next(message);
                            }
                        }
                    };
                    final String consumerTag = channel.basicConsume(queue, false, consumer);
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
            });
        }, options.getOverflowStrategy());
    }

    // TODO consume with dynamic QoS and/or batch ack

    public void close() {
        // TODO close emitted fluxes?
        // TODO make call idempotent
        try {
            connectionMono.block().close();
        } catch (IOException e) {
            throw new ReactorRabbitMqException(e);
        }
    }

    // TODO provide close method with Mono

}
