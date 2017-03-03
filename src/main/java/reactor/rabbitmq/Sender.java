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
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 *
 */
public class Sender {

    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

    private final Mono<Connection> connectionMono;

    private final Mono<Channel> channelMono;

    public Sender() {
        this(() -> {
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.useNio();
            return connectionFactory;
        });
    }

    public Sender(Supplier<ConnectionFactory> connectionFactorySupplier) {
        this(connectionFactorySupplier.get());
    }

    public Sender(ConnectionFactory connectionFactory) {
        this.connectionMono = Mono.fromCallable(() -> {
            Connection connection = connectionFactory.newConnection();
            return connection;
        }).cache();
        this.channelMono = Mono.fromCallable(() -> connectionMono.block().createChannel()).cache();
    }

    public Mono<Void> send(Publisher<OutboundMessage> messages) {
        // TODO using a pool of channels?
        // would be much more efficient if send is called very often
        // less useful if seldom called, only for long or infinite message flux
        final Mono<Channel> channelMono = connectionMono
            .then(connection -> Mono.fromCallable(() -> connection.createChannel()))
            .cache();
        return new Flux<Void>() {

            @Override
            public void subscribe(Subscriber<? super Void> s) {
                messages.subscribe(new BaseSubscriber<OutboundMessage>() {

                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        channelMono.block();
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

    private enum SubscriberState {
        INIT,
        ACTIVE,
        OUTBOUND_DONE,
        COMPLETE
    }

    public Flux<OutboundMessageResult> sendWithPublishConfirms(Publisher<OutboundMessage> messages) {
        // TODO using a pool of channels?
        // would be much more efficient if send is called very often
        // less useful if seldom called, only for long or infinite message flux
        final Mono<Channel> channelMono = connectionMono
            .then(connection -> Mono.fromCallable(() -> {
                Channel channel = connection.createChannel();
                channel.confirmSelect();
                return channel;
            }))
            .cache();

        ConcurrentNavigableMap<Long, OutboundMessage> unconfirmed = new ConcurrentSkipListMap<>();

        return new Flux<OutboundMessageResult>() {

            @Override
            public void subscribe(Subscriber<? super OutboundMessageResult> subscriber) {
                messages.subscribe(new Subscriber<OutboundMessage>() {

                    AtomicReference<SubscriberState> state = new AtomicReference<>(SubscriberState.INIT);

                    ExecutorService executorService = Executors.newFixedThreadPool(1);

                    @Override
                    public void onSubscribe(Subscription subscription) {
                        channelMono.block().addConfirmListener(new ConfirmListener() {
                            @Override
                            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                                handleAckNack(deliveryTag, multiple, true);
                            }

                            @Override
                            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                                handleAckNack(deliveryTag, multiple, false);
                            }

                            private void handleAckNack(long deliveryTag, boolean multiple, boolean ack) {
                                if(multiple) {
                                    ConcurrentNavigableMap<Long, OutboundMessage> unconfirmedToSend = unconfirmed.headMap(deliveryTag, true);
                                    Iterator<Map.Entry<Long, OutboundMessage>> iterator = unconfirmedToSend.entrySet().iterator();
                                    while(iterator.hasNext()) {
                                        subscriber.onNext(new OutboundMessageResult(iterator.next().getValue(), ack));
                                        iterator.remove();
                                    }
                                } else {
                                    OutboundMessage outboundMessage = unconfirmed.get(deliveryTag);
                                    unconfirmed.remove(deliveryTag);
                                    subscriber.onNext(new OutboundMessageResult(outboundMessage, ack));
                                }
                                if(unconfirmed.size() == 0) {
                                    executorService.submit(() -> {
                                        // confirmation listeners are executed in the IO reading thread
                                        // so we need to complete in another thread
                                        maybeComplete();
                                    });
                                }
                            }
                        });
                        state.set(SubscriberState.ACTIVE);
                        subscriber.onSubscribe(subscription);
                    }

                    @Override
                    public void onNext(OutboundMessage message) {
                        try {
                            unconfirmed.putIfAbsent(channelMono.block().getNextPublishSeqNo(), message);
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
                    public void onError(Throwable throwable) {
                        LOGGER.warn("Send failed with exception {}", throwable);
                    }

                    @Override
                    public void onComplete() {
                        if(state.compareAndSet(SubscriberState.ACTIVE, SubscriberState.OUTBOUND_DONE) && unconfirmed.size() == 0) {
                            maybeComplete();
                        }
                    }

                    private void maybeComplete() {
                        if(state.compareAndSet(SubscriberState.OUTBOUND_DONE, SubscriberState.COMPLETE)) {
                            try {
                                channelMono.block().close();
                            } catch (TimeoutException | IOException e) {
                                throw new ReactorRabbitMqException(e);
                            }
                            subscriber.onComplete();
                        }
                    }

                });
            }
        };
    }

    public Mono<AMQP.Queue.DeclareOk> createQueue(QueueSpecification specification) {
        return doOnChannel(channel -> {
            try {
                if(specification.getName() == null) {
                    return channel.queueDeclare();
                } else {
                    return channel.queueDeclare(
                        specification.getName(),
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
                    specification.getName(), specification.getType(),
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
        return Mono.fromCallable(() -> operation.apply(channel));
    }

    public void close() {
        // TODO make call idempotent
        try {
            connectionMono.block().close();
        } catch (IOException e) {
            throw new ReactorRabbitMqException(e);
        }
    }

    // TODO provide close method with Mono

}
