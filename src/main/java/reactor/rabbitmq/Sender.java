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
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

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

    // using specific scheduler to avoid being cancelled in subscribe
    // see https://github.com/reactor/reactor-core/issues/442
    private final Scheduler scheduler = Schedulers.fromExecutor(
        Executors.newFixedThreadPool(Schedulers.DEFAULT_POOL_SIZE),
        true
    );

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
        })
          .subscribeOn(scheduler)
          .cache();
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
            }));

        return channelMono.flatMapMany(channel -> new Flux<OutboundMessageResult>() {
            @Override
            public void subscribe(Subscriber<? super OutboundMessageResult> subscriber) {
                messages.subscribe(new PublishConfirmSubscriber(channel, subscriber));
            }
        });
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

    private static class PublishConfirmSubscriber implements Subscriber<OutboundMessage> {

        private final AtomicReference<SubscriberState> state = new AtomicReference<>(SubscriberState.INIT);

        private final ExecutorService executorService = Executors.newFixedThreadPool(1);

        private final AtomicReference<Throwable> firstException = new AtomicReference<Throwable>();

        private final ConcurrentNavigableMap<Long, OutboundMessage> unconfirmed = new ConcurrentSkipListMap<>();

        private final Channel channel;

        private final Subscriber<? super OutboundMessageResult> subscriber;

        private PublishConfirmSubscriber(Channel channel, Subscriber<? super OutboundMessageResult> subscriber) {
            this.channel = channel;
            this.subscriber = subscriber;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            channel.addConfirmListener(new ConfirmListener() {
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
                        try {
                            ConcurrentNavigableMap<Long, OutboundMessage> unconfirmedToSend = unconfirmed.headMap(deliveryTag, true);
                            Iterator<Map.Entry<Long, OutboundMessage>> iterator = unconfirmedToSend.entrySet().iterator();
                            while(iterator.hasNext()) {
                                subscriber.onNext(new OutboundMessageResult(iterator.next().getValue(), ack));
                                iterator.remove();
                            }
                        } catch(Exception e) {
                            handleError(e, null);
                        }
                    } else {
                        OutboundMessage outboundMessage = unconfirmed.get(deliveryTag);
                        try {
                            unconfirmed.remove(deliveryTag);
                            subscriber.onNext(new OutboundMessageResult(outboundMessage, ack));
                        } catch(Exception e) {
                            handleError(e, new OutboundMessageResult(outboundMessage, ack));
                        }

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
            if (checkComplete(message))
                return;

            long nextPublishSeqNo = channel.getNextPublishSeqNo();
            try {
                unconfirmed.putIfAbsent(nextPublishSeqNo, message);
                channel.basicPublish(
                    message.getExchange(),
                    message.getRoutingKey(),
                    message.getProperties(),
                    message.getBody()
                );
            } catch(Exception e) {
                unconfirmed.remove(nextPublishSeqNo);
                handleError(e, new OutboundMessageResult(message, false));
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (state.compareAndSet(SubscriberState.ACTIVE, SubscriberState.COMPLETE) ||
                state.compareAndSet(SubscriberState.OUTBOUND_DONE, SubscriberState.COMPLETE)) {
                closeResources();
                // complete the flux state
                subscriber.onError(throwable);
            } else if (firstException.compareAndSet(null, throwable) && state.get() == SubscriberState.COMPLETE) {
                // already completed, drop the error
                Operators.onErrorDropped(throwable);
            }
        }

        @Override
        public void onComplete() {
            if(state.compareAndSet(SubscriberState.ACTIVE, SubscriberState.OUTBOUND_DONE) && unconfirmed.size() == 0) {
                maybeComplete();
            }
        }

        private void handleError(Exception e, OutboundMessageResult result) {
            LOGGER.error("error in publish confirm sending", e);
            boolean complete = checkComplete(e);
            firstException.compareAndSet(null, e);
            if (!complete) {
                if(result != null) {
                    subscriber.onNext(result);
                }
                onError(e);
            }
        }

        private void maybeComplete() {
            if(state.compareAndSet(SubscriberState.OUTBOUND_DONE, SubscriberState.COMPLETE)) {
                closeResources();
                subscriber.onComplete();
            }
        }

        private void closeResources() {
            try {
                channel.close();
            } catch (TimeoutException | IOException e) {
                throw new ReactorRabbitMqException(e);
            }
        }

        public <T> boolean checkComplete(T t) {
            boolean complete = state.get() == SubscriberState.COMPLETE;
            if (complete && firstException.get() == null) {
                Operators.onNextDropped(t);
            }
            return complete;
        }

    }

}
