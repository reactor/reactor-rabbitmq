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
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class Receiver {

    private final Mono<Connection> connectionMono;

    public Receiver() {
        this.connectionMono = Mono.fromCallable(() -> {
            // TODO provide connection settings
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.useNio();
            // TODO handle exception
            Connection connection = connectionFactory.newConnection();
            return connection;
        }).cache();
    }

    // TODO more consumeNoAck functions:
    //  - with a Supplier<Boolean> or Predicate<FluxSink> or Predicate<Delivery> to complete the Flux

    public Flux<Delivery> consumeNoAck(final String queue) {
        // TODO handle overflow strategy
        // "IGNORE" should be fine for an auto-ack listener
        FluxSink.OverflowStrategy overflowStrategy = FluxSink.OverflowStrategy.IGNORE;
        Flux<Delivery> flux = Flux.create(unsafeEmitter -> {
            // because handleDelivery can be called from different threads
            FluxSink<Delivery> emitter = unsafeEmitter.serialize();
            // TODO handle timeout
            Connection connection = connectionMono.block();
            try {
                // TODO handle exception
                Channel channel = connection.createChannel();
                final DefaultConsumer consumer = new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        emitter.next(new Delivery(envelope, properties, body));
                    }
                };
                final String consumerTag = channel.basicConsume(queue, true, consumer);
                emitter.setCancellation(() -> {
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
        }, overflowStrategy);
        // TODO track flux so it can be disposed when the sender is closed?
        // could be also developer responsibility
        return flux;
    }

    public Flux<Delivery> consumeAutoAck(final String queue) {
        // TODO why acking here and not just after emitter.next()?
        return consumeManuelAck(queue).doOnNext(msg -> msg.ack()).map(ackableMsg -> (Delivery) ackableMsg);
    }

    // TODO add consumeManualAck method

    public Flux<AcknowledgableDelivery> consumeManuelAck(final String queue) {
        // TODO handle overflow strategy
        FluxSink.OverflowStrategy overflowStrategy = FluxSink.OverflowStrategy.BUFFER;

        // TODO handle QoS

        Flux<AcknowledgableDelivery> flux = Flux.create(unsafeEmitter -> {
            // because handleDelivery can be called from different threads
            FluxSink<AcknowledgableDelivery> emitter = unsafeEmitter.serialize();
            // TODO handle timeout
            Connection connection = connectionMono.block();
            try {
                // TODO handle exception
                Channel channel = connection.createChannel();
                final DefaultConsumer consumer = new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        emitter.next(new AcknowledgableDelivery(envelope, properties, body, getChannel()));
                    }
                };
                final String consumerTag = channel.basicConsume(queue, false, consumer);
                emitter.setCancellation(() -> {
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
        }, overflowStrategy);
        // TODO track flux so it can be disposed when the sender is closed?
        // could be also developer responsibility
        return flux;
    }

    public void close() {
        // TODO close emitted fluxes?
        try {
            connectionMono.block().close();
        } catch (IOException e) {
            throw new ReactorRabbitMqException(e);
        }
    }

}
