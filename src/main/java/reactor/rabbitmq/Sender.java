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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 *
 */
public class Sender {

    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

    private final Mono<Connection> connectionMono;

    private final Mono<Channel> channelMono;

    public Sender() {
        this.connectionMono = Mono.fromCallable(() -> {
            // TODO provide connection settings
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.useNio();
            // TODO handle exception
            Connection connection = connectionFactory.newConnection();
            return connection;
        }).cache();
        this.channelMono = Mono.fromCallable(() -> connectionMono.block().createChannel()).cache();
    }

    public Mono<Void> send(Publisher<OutboundMessage> messages) {
        // TODO using a pool of channels?
        // would be much more efficient if send is called very often
        // less useful if seldom called, only for long or infinite message flux
        final Mono<Channel> channelMono = Mono.fromCallable(() -> connectionMono.block().createChannel()).cache();
        return new Flux<Void>() {

            @Override
            public void subscribe(Subscriber<? super Void> s) {
                messages.subscribe(new BaseSubscriber<OutboundMessage>() {

                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        s.onSubscribe(subscription);
                    }

                    @Override
                    protected void hookOnNext(OutboundMessage message) {
                        try {
                            channelMono.block().basicPublish(
                                message.getExchange(),
                                message.getRoutingKey(),
                                message.getProperties(),
                                message.getBody()
                            );
                        } catch(IOException e) {
                            throw new ReactorRabbitMqException(e);
                        }
                    }

                    @Override
                    protected void hookOnError(Throwable throwable) {
                        LOGGER.warn("Send failed with exception {}", throwable);
                    }

                    @Override
                    protected void hookOnComplete() {
                        try {
                            channelMono.block().close();
                        } catch (TimeoutException | IOException e) {
                            throw new ReactorRabbitMqException(e);
                        }
                        s.onComplete();
                    }
                });
            }
        }.then();
    }

    public Mono<AMQP.Queue.DeclareOk> createQueue(QueueSpecification specification) {
        return doOnChannel(channel -> {
            try {
                if(specification.getQueue() == null) {
                    return channel.queueDeclare();
                } else {
                    return channel.queueDeclare(
                        specification.getQueue(),
                        specification.isDurable(),
                        specification.isExclusive(),
                        specification.isAutoDelete(),
                        specification.getArguments()
                    );
                }
            } catch (IOException e) {
                throw new ReactorRabbitMqException(e);
            }
        }, channelMono.block());
    }

    public Mono<AMQP.Exchange.DeclareOk> createExchange(ExchangeSpecification specification) {
        return doOnChannel(channel -> {
            try {
                return channel.exchangeDeclare(
                    specification.getExchange(), specification.getType(),
                    specification.isDurable(),
                    specification.isAutoDelete(),
                    specification.isInternal(),
                    specification.getArguments()
                );
            } catch (IOException e) {
                throw new ReactorRabbitMqException(e);
            }
        }, channelMono.block());
    }

    public Mono<AMQP.Queue.BindOk> bind(BindingSpecification specification) {
        return doOnChannel(channel -> {
            try {
                return channel.queueBind(
                    specification.getQueue(), specification.getExchange(),
                    specification.getRoutingKey(), specification.getArguments());
            } catch (IOException e) {
                throw new ReactorRabbitMqException(e);
            }
        }, channelMono.block());
    }

    public static <T> Mono<T> doOnChannel(Function<Channel, T> operation, Channel channel) {
        return Mono.create(emitter -> {
            try {
                T result = operation.apply(channel);
                emitter.success(result);
            } catch(Exception e) {
                emitter.error(e);
            }

        });
    }

    public void close() {
        try {
            connectionMono.block().close();
        } catch (IOException e) {
            throw new ReactorRabbitMqException(e);
        }
    }

    // TODO provide close method with Mono

}
