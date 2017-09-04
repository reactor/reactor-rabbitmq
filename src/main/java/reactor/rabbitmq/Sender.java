/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
import com.rabbitmq.client.Command;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.impl.AMQImpl;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
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

    private final ExecutorService resourceCreationExecutorService = Executors.newSingleThreadExecutor();

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
        final Channel channel;
        try {
            channel = connectionMono.block().createChannel();
        } catch (IOException e) {
            throw new ReactorRabbitMqException(e);
        }
        return new Flux<Void>() {

            @Override
            public void subscribe(CoreSubscriber<? super Void> s) {
                messages.subscribe(new BaseSubscriber<OutboundMessage>() {

                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        s.onSubscribe(subscription);
                    }

                    @Override
                    protected void hookOnNext(OutboundMessage message) {
                        try {
                            channel.basicPublish(
                                message.getExchange(),
                                message.getRoutingKey(),
                                message.getProperties(),
                                message.getBody()
                            );
                        } catch (IOException e) {
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
                            LOGGER.info("closing channel {}", channel.getChannelNumber());
                            channel.close();
                        } catch (TimeoutException | IOException e) {
                            e.printStackTrace();
                            LOGGER.warn("Channel {} didn't close normally: {}", channel.getChannelNumber(), e.getMessage());
                        }
                        s.onComplete();
                    }
                });
            }
        }.then();
    }

    public Flux<OutboundMessageResult> sendWithPublishConfirms(Publisher<OutboundMessage> messages) {
        // TODO using a pool of channels?
        // would be much more efficient if send is called very often
        // less useful if seldom called, only for long or infinite message flux
        final Mono<Channel> channelMono = connectionMono
            .flatMap(connection -> {
                Channel channel = null;
                try {
                    channel = connection.createChannel();
                    channel.confirmSelect();
                    return Mono.just(channel);
                } catch (IOException e) {
                    return Mono.error(e);
                }
            });

        return channelMono.flatMapMany(channel -> new Flux<OutboundMessageResult>() {

            @Override
            public void subscribe(CoreSubscriber<? super OutboundMessageResult>
                subscriber) {
                messages.subscribe(new PublishConfirmSubscriber(channel, subscriber));
            }
        });
    }

    public Mono<AMQP.Queue.DeclareOk> createQueue(QueueSpecification specification) {
        AMQP.Queue.Declare declare;
        if (specification.getName() == null) {
            declare = new AMQImpl.Queue.Declare.Builder()
                .queue("")
                .durable(false)
                .exclusive(true)
                .autoDelete(true)
                .arguments(null)
                .build();
        } else {
            declare = new AMQImpl.Queue.Declare.Builder()
                .queue(specification.getName())
                .durable(specification.isDurable())
                .exclusive(specification.isExclusive())
                .autoDelete(specification.isAutoDelete())
                .arguments(specification.getArguments())
                .build();
        }
        Callable<CompletableFuture<Command>> creation = () -> channelMono.block().asyncCompletableRpc(declare, null);
        return Mono.fromCallable(creation)
            .flatMap(future -> Mono.fromCompletionStage(future))
            .flatMap(command -> Mono.just((AMQP.Queue.DeclareOk) command.getMethod()));
    }

    public Mono<AMQP.Exchange.DeclareOk> createExchange(ExchangeSpecification specification) {
        Callable<CompletableFuture<Command>> creation = () -> {
            CompletableFuture<Command> completableFuture = channelMono.block().asyncCompletableRpc(new AMQImpl.Exchange.Declare.Builder()
                .exchange(specification.getName())
                .type(specification.getType())
                .durable(specification.isDurable())
                .autoDelete(specification.isAutoDelete())
                .internal(specification.isInternal())
                .arguments(specification.getArguments())
                .build(), null);

            return completableFuture;
        };

        return Mono.fromCallable(creation)
            .flatMap(future -> Mono.fromCompletionStage(future))
            .flatMap(command -> Mono.just((AMQP.Exchange.DeclareOk) command.getMethod()));
    }

    public Mono<AMQP.Queue.BindOk> bind(BindingSpecification specification) {
        Callable<CompletableFuture<Command>> creation = () -> {
            return channelMono.block().asyncCompletableRpc(
                new AMQImpl.Queue.Bind.Builder()
                    .exchange(specification.getExchange())
                    .queue(specification.getQueue())
                    .routingKey(specification.getRoutingKey())
                    .arguments(specification.getArguments())
                    .build(), null);
        };

        return Mono.fromCallable(creation)
            .flatMap(completableFuture -> Mono.fromCompletionStage(completableFuture))
            .flatMap(command -> Mono.just((AMQP.Queue.BindOk) command.getMethod()));
    }

    public void close() {
        // TODO make call idempotent
        try {
            connectionMono.block().close();
            resourceCreationExecutorService.shutdownNow();
        } catch (IOException e) {
            throw new ReactorRabbitMqException(e);
        }
    }

    private enum SubscriberState {
        INIT,
        ACTIVE,
        OUTBOUND_DONE,
        COMPLETE
    }

    // TODO provide close method with Mono

    private static class PublishConfirmSubscriber implements
        CoreSubscriber<OutboundMessage> {

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
                    if (multiple) {
                        try {
                            ConcurrentNavigableMap<Long, OutboundMessage> unconfirmedToSend = unconfirmed.headMap(deliveryTag, true);
                            Iterator<Map.Entry<Long, OutboundMessage>> iterator = unconfirmedToSend.entrySet().iterator();
                            while (iterator.hasNext()) {
                                subscriber.onNext(new OutboundMessageResult(iterator.next().getValue(), ack));
                                iterator.remove();
                            }
                        } catch (Exception e) {
                            handleError(e, null);
                        }
                    } else {
                        OutboundMessage outboundMessage = unconfirmed.get(deliveryTag);
                        try {
                            unconfirmed.remove(deliveryTag);
                            subscriber.onNext(new OutboundMessageResult(outboundMessage, ack));
                        } catch (Exception e) {
                            handleError(e, new OutboundMessageResult(outboundMessage, ack));
                        }
                    }
                    if (unconfirmed.size() == 0) {
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
            } catch (Exception e) {
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
            if (state.compareAndSet(SubscriberState.ACTIVE, SubscriberState.OUTBOUND_DONE) && unconfirmed.size() == 0) {
                maybeComplete();
            }
        }

        private void handleError(Exception e, OutboundMessageResult result) {
            LOGGER.error("error in publish confirm sending", e);
            boolean complete = checkComplete(e);
            firstException.compareAndSet(null, e);
            if (!complete) {
                if (result != null) {
                    subscriber.onNext(result);
                }
                onError(e);
            }
        }

        private void maybeComplete() {
            if (state.compareAndSet(SubscriberState.OUTBOUND_DONE, SubscriberState.COMPLETE)) {
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
