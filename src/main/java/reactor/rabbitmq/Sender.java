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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.impl.AMQImpl;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Reactive abstraction to create resources and send messages.
 */
public class Sender implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

    private static final Function<Connection, Channel> CHANNEL_CREATION_FUNCTION = new ChannelCreationFunction();

    private final Mono<? extends Connection> connectionMono;

    private final AtomicBoolean hasConnection = new AtomicBoolean(false);

    private final Mono<Channel> channelMono;

    private final Scheduler resourceCreationScheduler;

    private final boolean privateResourceCreationScheduler;

    private final Scheduler connectionSubscriptionScheduler;

    private final boolean privateConnectionSubscriptionScheduler;

    public Sender() {
        this(new SenderOptions());
    }

    public Sender(SenderOptions options) {
        this.privateConnectionSubscriptionScheduler = options.getConnectionSubscriptionScheduler() == null;
        this.connectionSubscriptionScheduler = options.getConnectionSubscriptionScheduler() == null ?
            createScheduler("rabbitmq-sender-connection-subscription") : options.getConnectionSubscriptionScheduler();
        this.connectionMono = options.getConnectionMono() != null ? options.getConnectionMono() :
            Mono.fromCallable(() -> options.getConnectionFactory().newConnection())
                .doOnSubscribe(c -> hasConnection.set(true))
                .subscribeOn(this.connectionSubscriptionScheduler)
                .cache();
        this.privateResourceCreationScheduler = options.getResourceCreationScheduler() == null;
        this.resourceCreationScheduler = options.getResourceCreationScheduler() == null ?
            createScheduler("rabbitmq-sender-resource-creation") : options.getResourceCreationScheduler();
        this.channelMono = connectionMono.map(CHANNEL_CREATION_FUNCTION).cache();
    }

    protected Scheduler createScheduler(String name) {
        return Schedulers.newElastic(name);
    }

    public Mono<Void> send(Publisher<OutboundMessage> messages) {
        return send(messages, new SendOptions());
    }

    public Mono<Void> send(Publisher<OutboundMessage> messages, SendOptions options) {
        // TODO using a pool of channels?
        // would be much more efficient if send is called very often
        // less useful if seldom called, only for long or infinite message flux
        final Mono<Channel> currentChannelMono = connectionMono.map(CHANNEL_CREATION_FUNCTION).cache();
        final BiConsumer<SendContext, Exception> exceptionHandler = options.getExceptionHandler();
        return currentChannelMono.flatMapMany(channel ->
            Flux.from(messages)
                .doOnNext(message -> {
                    try {
                        channel.basicPublish(
                            message.getExchange(),
                            message.getRoutingKey(),
                            message.getProperties(),
                            message.getBody()
                        );
                    } catch (Exception e) {
                        exceptionHandler.accept(new SendContext(channel, message), e);
                    }
                })
                .doOnError(e -> LOGGER.warn("Send failed with exception {}", e))
                .doFinally(st -> {
                    int channelNumber = channel.getChannelNumber();
                    LOGGER.info("closing channel {} by signal {}", channelNumber, st);
                    try {
                        if (channel.isOpen() && channel.getConnection().isOpen()) {
                            channel.close();
                        }
                    } catch (Exception e) {
                        LOGGER.warn("Channel {} didn't close normally: {}", channelNumber, e.getMessage());
                    }
                })
        ).then();
    }

    public Flux<OutboundMessageResult> sendWithPublishConfirms(Publisher<OutboundMessage> messages) {
        return sendWithPublishConfirms(messages, new SendOptions());
    }

    public Flux<OutboundMessageResult> sendWithPublishConfirms(Publisher<OutboundMessage> messages, SendOptions options) {
        // TODO using a pool of channels?
        // would be much more efficient if send is called very often
        // less useful if seldom called, only for long or infinite message flux
        final Mono<Channel> channelMono = connectionMono.map(CHANNEL_CREATION_FUNCTION)
            .map(channel -> {
                try {
                    channel.confirmSelect();
                } catch (IOException e) {
                    throw new ReactorRabbitMqException("Error while setting publisher confirms on channel", e);
                }
                return channel;
            });

        return channelMono.flatMapMany(channel -> new PublishConfirmOperator(messages, channel, options));
    }

    public RpcClient rpcClient(String exchange, String routingKey) {
        return new RpcClient(connectionMono.map(CHANNEL_CREATION_FUNCTION).cache(), exchange, routingKey);
    }

    public RpcClient rpcClient(String exchange, String routingKey, Supplier<String> correlationIdProvider) {
        return new RpcClient(connectionMono.map(CHANNEL_CREATION_FUNCTION).cache(), exchange, routingKey, correlationIdProvider);
    }

    public Mono<AMQP.Queue.DeclareOk> declare(QueueSpecification specification) {
        return this.declareQueue(specification);
    }

    public Mono<AMQP.Queue.DeclareOk> declareQueue(QueueSpecification specification) {
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

        return channelMono.map(channel -> {
            try {
                return channel.asyncCompletableRpc(declare);
            } catch (IOException e) {
                throw new ReactorRabbitMqException("Error during RPC call", e);
            }
        }).flatMap(future -> Mono.fromCompletionStage(future))
            .flatMap(command -> Mono.just((AMQP.Queue.DeclareOk) command.getMethod()))
            .publishOn(resourceCreationScheduler);
    }

    public Mono<AMQP.Queue.DeleteOk> delete(QueueSpecification specification) {
        return this.delete(specification, false, false);
    }

    public Mono<AMQP.Queue.DeleteOk> delete(QueueSpecification specification, boolean ifUnused, boolean ifEmpty) {
        return this.deleteQueue(specification, ifUnused, ifEmpty);
    }

    public Mono<AMQP.Queue.DeleteOk> deleteQueue(QueueSpecification specification, boolean ifUnused, boolean ifEmpty) {
        AMQP.Queue.Delete delete = new AMQImpl.Queue.Delete.Builder()
            .queue(specification.getName())
            .ifUnused(ifUnused)
            .ifEmpty(ifEmpty)
            .build();

        return channelMono.map(channel -> {
            try {
                return channel.asyncCompletableRpc(delete);
            } catch (IOException e) {
                throw new ReactorRabbitMqException("Error during RPC call", e);
            }
        }).flatMap(future -> Mono.fromCompletionStage(future))
            .flatMap(command -> Mono.just((AMQP.Queue.DeleteOk) command.getMethod()))
            .publishOn(resourceCreationScheduler);
    }

    public Mono<AMQP.Exchange.DeclareOk> declare(ExchangeSpecification specification) {
        return this.declareExchange(specification);
    }

    public Mono<AMQP.Exchange.DeclareOk> declareExchange(ExchangeSpecification specification) {
        AMQP.Exchange.Declare declare = new AMQImpl.Exchange.Declare.Builder()
            .exchange(specification.getName())
            .type(specification.getType())
            .durable(specification.isDurable())
            .autoDelete(specification.isAutoDelete())
            .internal(specification.isInternal())
            .arguments(specification.getArguments())
            .build();
        return channelMono.map(channel -> {
            try {
                return channel.asyncCompletableRpc(declare);
            } catch (IOException e) {
                throw new ReactorRabbitMqException("Error during RPC call", e);
            }
        }).flatMap(future -> Mono.fromCompletionStage(future))
            .flatMap(command -> Mono.just((AMQP.Exchange.DeclareOk) command.getMethod()))
            .publishOn(resourceCreationScheduler);
    }

    public Mono<AMQP.Exchange.DeleteOk> delete(ExchangeSpecification specification) {
        return this.delete(specification, false);
    }

    public Mono<AMQP.Exchange.DeleteOk> delete(ExchangeSpecification specification, boolean ifUnused) {
        return this.deleteExchange(specification, ifUnused);
    }

    public Mono<AMQP.Exchange.DeleteOk> deleteExchange(ExchangeSpecification specification, boolean ifUnused) {
        AMQP.Exchange.Delete delete = new AMQImpl.Exchange.Delete.Builder()
            .exchange(specification.getName())
            .ifUnused(ifUnused)
            .build();
        return channelMono.map(channel -> {
            try {
                return channel.asyncCompletableRpc(delete);
            } catch (IOException e) {
                throw new ReactorRabbitMqException("Error during RPC call", e);
            }
        }).flatMap(future -> Mono.fromCompletionStage(future))
            .flatMap(command -> Mono.just((AMQP.Exchange.DeleteOk) command.getMethod()))
            .publishOn(resourceCreationScheduler);
    }

    public Mono<AMQP.Queue.UnbindOk> unbind(BindingSpecification specification) {
        AMQP.Queue.Unbind unbinding = new AMQImpl.Queue.Unbind.Builder()
            .exchange(specification.getExchange())
            .queue(specification.getQueue())
            .routingKey(specification.getRoutingKey())
            .arguments(specification.getArguments())
            .build();

        return channelMono.map(channel -> {
            try {
                return channel.asyncCompletableRpc(unbinding);
            } catch (IOException e) {
                throw new ReactorRabbitMqException("Error during RPC call", e);
            }
        }).flatMap(future -> Mono.fromCompletionStage(future))
            .flatMap(command -> Mono.just((AMQP.Queue.UnbindOk) command.getMethod()))
            .publishOn(resourceCreationScheduler);
    }

    public Mono<AMQP.Queue.BindOk> bind(BindingSpecification specification) {
        AMQP.Queue.Bind binding = new AMQImpl.Queue.Bind.Builder()
            .exchange(specification.getExchange())
            .queue(specification.getQueue())
            .routingKey(specification.getRoutingKey())
            .arguments(specification.getArguments())
            .build();

        return channelMono.map(channel -> {
            try {
                return channel.asyncCompletableRpc(binding);
            } catch (IOException e) {
                throw new ReactorRabbitMqException("Error during RPC call", e);
            }
        }).flatMap(future -> Mono.fromCompletionStage(future))
            .flatMap(command -> Mono.just((AMQP.Queue.BindOk) command.getMethod()))
            .publishOn(resourceCreationScheduler);
    }

    public void close() {
        if (hasConnection.getAndSet(false)) {
            try {
                // FIXME use timeout on block (should be a parameter of the Sender)
                connectionMono.block().close();
            } catch (IOException e) {
                throw new ReactorRabbitMqException(e);
            }
        }
        if (this.privateConnectionSubscriptionScheduler) {
            this.connectionSubscriptionScheduler.dispose();
        }
        if (this.privateResourceCreationScheduler) {
            this.resourceCreationScheduler.dispose();
        }
    }

    public static class SendContext {

        protected final Channel channel;
        protected final OutboundMessage message;

        public SendContext(Channel channel, OutboundMessage message) {
            this.channel = channel;
            this.message = message;
        }

        public OutboundMessage getMessage() {
            return message;
        }

        public Channel getChannel() {
            return channel;
        }

        public void publish(OutboundMessage outboundMessage) throws Exception {
            this.channel.basicPublish(
                outboundMessage.getExchange(),
                outboundMessage.getRoutingKey(),
                outboundMessage.getProperties(),
                outboundMessage.getBody()
            );
        }

        public void publish() throws Exception {
            this.publish(getMessage());
        }
    }

    public static class ConfirmSendContext extends SendContext {

        private final PublishConfirmSubscriber subscriber;

        public ConfirmSendContext(Channel channel, OutboundMessage message, PublishConfirmSubscriber subscriber) {
            super(channel, message);
            this.subscriber = subscriber;
        }

        @Override
        public void publish(OutboundMessage outboundMessage) throws Exception {
            long nextPublishSeqNo = channel.getNextPublishSeqNo();
            try {
                subscriber.unconfirmed.putIfAbsent(nextPublishSeqNo, this.message);
                super.publish(outboundMessage);
            } catch (Exception e) {
                subscriber.unconfirmed.remove(nextPublishSeqNo);
                throw e;
            }
        }

        @Override
        public void publish() throws Exception {
            this.publish(getMessage());
        }
    }



    private static class PublishConfirmOperator
        extends FluxOperator<OutboundMessage, OutboundMessageResult> {

        private final Channel channel;

        private final SendOptions options;

        public PublishConfirmOperator(Publisher<OutboundMessage> source, Channel channel, SendOptions options) {
            super(Flux.from(source));
            this.channel = channel;
            this.options = options;
        }

        @Override
        public void subscribe(CoreSubscriber<? super OutboundMessageResult> actual) {
            source.subscribe(new PublishConfirmSubscriber(channel, actual, options));
        }
    }

    private static class PublishConfirmSubscriber implements
        CoreSubscriber<OutboundMessage> {

        private final AtomicReference<SubscriberState> state = new AtomicReference<>(SubscriberState.INIT);

        private final AtomicReference<Throwable> firstException = new AtomicReference<Throwable>();

        private final ConcurrentNavigableMap<Long, OutboundMessage> unconfirmed = new ConcurrentSkipListMap<>();

        private final Channel channel;

        private final Subscriber<? super OutboundMessageResult> subscriber;

        private final BiConsumer<SendContext, Exception> exceptionHandler;

        private PublishConfirmSubscriber(Channel channel, Subscriber<? super OutboundMessageResult> subscriber, SendOptions options) {
            this.channel = channel;
            this.subscriber = subscriber;
            this.exceptionHandler = options.getExceptionHandler();
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
                        // FIXME create new thread only if needed, likely in maybeComplete()
                        new Thread(() -> {
                            // confirmation listeners are executed in the IO reading thread
                            // so we need to complete in another thread
                            maybeComplete();
                        }).start();
                    }
                }
            });
            state.set(SubscriberState.ACTIVE);
            subscriber.onSubscribe(subscription);
        }

        @Override
        public void onNext(OutboundMessage message) {
            if (checkComplete(message)) {
                return;
            }

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
                try {
                    this.exceptionHandler.accept(new ConfirmSendContext(channel, message, this), e);
                } catch (Exception innerException) {
                    handleError(innerException, new OutboundMessageResult(message, false));
                }
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
                Operators.onErrorDropped(throwable, currentContext());
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
            boolean done = state.compareAndSet(SubscriberState.OUTBOUND_DONE, SubscriberState.COMPLETE);
            if (done) {
                closeResources();
                subscriber.onComplete();
            }
        }

        private void closeResources() {
            try {
                if (channel.isOpen()) {
                    channel.close();
                }
            } catch (Exception e) {
                // not much we can do here
            }
        }

        public <T> boolean checkComplete(T t) {
            boolean complete = state.get() == SubscriberState.COMPLETE;
            if (complete && firstException.get() == null) {
                Operators.onNextDropped(t, currentContext());
            }
            return complete;
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
