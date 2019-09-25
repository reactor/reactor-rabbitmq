/*
 * Copyright (c) 2017-2019 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.rabbitmq;

import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.AMQImpl;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.*;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static reactor.rabbitmq.Helpers.safelyExecute;

/**
 * Reactive abstraction to create resources and send messages.
 */
public class Sender implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

    private static final Function<Connection, Channel> CHANNEL_CREATION_FUNCTION = new ChannelCreationFunction();

    private static final Function<Connection, Channel> CHANNEL_PROXY_CREATION_FUNCTION = new ChannelProxyCreationFunction();

    private final Mono<? extends Connection> connectionMono;

    private final Mono<? extends Channel> channelMono;

    private final BiConsumer<SignalType, Channel> channelCloseHandler;

    /**
     * To track the cached connection when {@link #connectionMono} is not provided.
     */
    private final AtomicReference<Connection> connection = new AtomicReference<>();

    private final Mono<? extends Channel> resourceManagementChannelMono;

    private final Scheduler resourceManagementScheduler;

    private final boolean privateResourceManagementScheduler;

    private final Scheduler connectionSubscriptionScheduler;

    private final boolean privateConnectionSubscriptionScheduler;

    private final ExecutorService channelCloseThreadPool = Executors.newCachedThreadPool();

    private final int connectionClosingTimeout;

    private final AtomicBoolean closingOrClosed = new AtomicBoolean(false);

    private static final String REACTOR_RABBITMQ_DELIVERY_TAG_HEADER = "reactor_rabbitmq_delivery_tag";

    public Sender() {
        this(new SenderOptions());
    }

    public Sender(SenderOptions options) {
        this.privateConnectionSubscriptionScheduler = options.getConnectionSubscriptionScheduler() == null;
        this.connectionSubscriptionScheduler = options.getConnectionSubscriptionScheduler() == null ?
            createScheduler("rabbitmq-sender-connection-subscription") : options.getConnectionSubscriptionScheduler();

        Mono<? extends Connection> cm;
        if (options.getConnectionMono() == null) {
            cm = Mono.fromCallable(() -> {
                if (options.getConnectionSupplier() == null) {
                    return options.getConnectionFactory().newConnection();
                } else {
                    // the actual connection factory to use is already set in a function wrapper, not need to use one
                    return options.getConnectionSupplier().apply(null);
                }
            });
            cm = options.getConnectionMonoConfigurator().apply(cm);
            cm = cm.doOnNext(conn -> connection.set(conn))
                    .subscribeOn(this.connectionSubscriptionScheduler)
                    .transform(this::cache);
        } else {
            cm = options.getConnectionMono();
        }

        this.connectionMono = cm;
        this.channelMono = options.getChannelMono();
        this.channelCloseHandler = options.getChannelCloseHandler() == null ?
                ChannelCloseHandlers.SENDER_CHANNEL_CLOSE_HANDLER_INSTANCE :
                options.getChannelCloseHandler();
        this.privateResourceManagementScheduler = options.getResourceManagementScheduler() == null;
        this.resourceManagementScheduler = options.getResourceManagementScheduler() == null ?
            createScheduler("rabbitmq-sender-resource-creation") : options.getResourceManagementScheduler();
        this.resourceManagementChannelMono = options.getResourceManagementChannelMono() == null ?
            connectionMono.map(CHANNEL_PROXY_CREATION_FUNCTION).transform(this::cache) : options.getResourceManagementChannelMono();
        if (options.getConnectionClosingTimeout() != null && !Duration.ZERO.equals(options.getConnectionClosingTimeout())) {
            this.connectionClosingTimeout = (int) options.getConnectionClosingTimeout().toMillis();
        } else {
            this.connectionClosingTimeout = -1;
        }
    }

    protected Scheduler createScheduler(String name) {
        return Schedulers.newElastic(name);
    }

    protected <T> Mono<T> cache(Mono<T> mono) {
        return Utils.cache(mono);
    }

    public Mono<Void> send(Publisher<OutboundMessage> messages) {
        return send(messages, new SendOptions());
    }

    public Mono<Void> send(Publisher<OutboundMessage> messages, SendOptions options) {
        options = options == null ? new SendOptions() : options;
        final Mono<? extends Channel> currentChannelMono = getChannelMono(options);
        final BiConsumer<SendContext, Exception> exceptionHandler = options.getExceptionHandler();
        final BiConsumer<SignalType, Channel> channelCloseHandler = getChannelCloseHandler(options);

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
                        exceptionHandler.accept(new SendContext<Object>(channel, message), e);
                    }
                })
                .doOnError(e -> LOGGER.warn("Send failed with exception {}", e))
                .doFinally(st -> channelCloseHandler.accept(st, channel))
        ).then();
    }

    /**
     * Publish a flux of messages and expect confirmations.
     *
     * <p>
     * This method uses <a href="https://www.rabbitmq.com/confirms.html#publisher-confirms">RabbitMQ Publisher
     * Confirms</a> extension to make sure
     * outbound messages made it or not to the broker.
     * <p>
     * See {@link #sendWithPublishConfirms(Publisher, SendOptions)} to have more control over the publishing
     * and the confirmations with {@link SendOptions}.
     *
     * @param messages flux of outbound messages
     * @return flux of confirmations
     * @see <a href="https://www.rabbitmq.com/confirms.html#publisher-confirms">Publisher Confirms</a>
     */
    public Flux<OutboundMessageResult> sendWithPublishConfirms(Publisher<OutboundMessage> messages) {
        return sendWithPublishConfirms(messages, new SendOptions());
    }

    /**
     * Publish a flux of messages and expect confirmations.
     * <p>
     * This method uses <a href="https://www.rabbitmq.com/confirms.html#publisher-confirms">RabbitMQ Publisher
     * Confirms</a> extension to make sure
     * outbound messages made it or not to the broker.
     * <p>
     * It is also possible to know if a message has been routed to a least one queue
     * by enabling the <a href="https://www.rabbitmq.com/publishers.html#unroutable">mandatory flag</a>. The
     * default is to not use this flag.
     *
     * @param messages flux of outbound messages
     * @param options  options to configure publishing
     * @return flux of confirmations
     * @see <a href="https://www.rabbitmq.com/confirms.html#publisher-confirms">Publisher Confirms</a>
     * @see <a href="https://www.rabbitmq.com/publishers.html#unroutable">Mandatory flag</a>
     * @see SendOptions#trackReturned(boolean)
     */
    public Flux<OutboundMessageResult> sendWithPublishConfirms(Publisher<OutboundMessage> messages, SendOptions options) {
        return sendWithCorrelatedPublishConfirms(Flux.from(messages).map(message -> (OutboundMessage<Object>) message), options)
            .map(Function.identity());
    }

    /**
     * Publish a flux of messages with typed correlated metadata and expect confirmations.
     *
     * <p>
     * This method uses <a href="https://www.rabbitmq.com/confirms.html#publisher-confirms">RabbitMQ Publisher
     * Confirms</a> extension to make sure
     * outbound messages made it or not to the broker.
     * <p>
     * See {@link #sendWithCorrelatedPublishConfirms(Publisher, SendOptions)} to have more control over the
     * publishing and the confirmations with {@link SendOptions}.
     *
     * @param messages flux of outbound messages with typed correlated metadata
     * @return flux of confirmations with typed correlated metadata
     * @see <a href="https://www.rabbitmq.com/confirms.html#publisher-confirms">Publisher Confirms</a>
     */
    public <T> Flux<OutboundMessageResult<T>> sendWithCorrelatedPublishConfirms(Publisher<OutboundMessage<T>> messages) {
        return sendWithCorrelatedPublishConfirms(messages, new SendOptions());
    }

    /**
     * Publish a flux of messages with typed correlated metadata and expect confirmations.
     * <p>
     * This method uses <a href="https://www.rabbitmq.com/confirms.html#publisher-confirms">RabbitMQ Publisher
     * Confirms</a> extension to make sure
     * outbound messages made it or not to the broker.
     * <p>
     * It is also possible to know if a message has been routed to a least one queue
     * by enabling the <a href="https://www.rabbitmq.com/publishers.html#unroutable">mandatory flag</a>. The
     * default is to not use this flag.
     *
     * @param messages flux of outbound messages with typed correlated metadata
     * @param options  options to configure publishing
     * @return flux of confirmations
     * @see <a href="https://www.rabbitmq.com/confirms.html#publisher-confirms">Publisher Confirms</a>
     * @see <a href="https://www.rabbitmq.com/publishers.html#unroutable">Mandatory flag</a>
     * @see SendOptions#trackReturned(boolean)
     */
    public <T> Flux<OutboundMessageResult<T>> sendWithCorrelatedPublishConfirms(Publisher<OutboundMessage<T>> messages, SendOptions options) {
        SendOptions sendOptions = options == null ? new SendOptions() : options;
        final Mono<? extends Channel> currentChannelMono = getChannelMono(options);
        final BiConsumer<SignalType, Channel> channelCloseHandler = getChannelCloseHandler(options);

        Flux<OutboundMessageResult<T>> result = currentChannelMono.map(channel -> {
                try {
                    channel.confirmSelect();
                } catch (IOException e) {
                    throw new RabbitFluxException("Error while setting publisher confirms on channel", e);
                }
                return channel;
            })
            .flatMapMany(channel -> new PublishConfirmOperator<>(messages, channel, sendOptions).doFinally(signalType -> {
                // channel closing is done here, to avoid creating threads inside PublishConfirmOperator,
                // which would make ChannelPool useless
                if (signalType == SignalType.ON_ERROR) {
                    channelCloseHandler.accept(signalType, channel);
                } else {
                    // confirmation listeners are executed in the IO reading thread
                    // so we need to complete in another thread
                    channelCloseThreadPool.execute(() -> channelCloseHandler.accept(signalType, channel));
                }
            }));

        if (sendOptions.getMaxInFlight() != null) {
            result = result.publishOn(sendOptions.getScheduler(), sendOptions.getMaxInFlight());
        }
        return result;
    }

    // package-protected for testing
    Mono<? extends Channel> getChannelMono(SendOptions options) {
        return Stream.of(options.getChannelMono(), channelMono)
                .filter(Objects::nonNull)
                .findFirst().orElse(connectionMono.map(CHANNEL_CREATION_FUNCTION));
    }

    private BiConsumer<SignalType, Channel> getChannelCloseHandler(SendOptions options) {
        return options.getChannelCloseHandler() != null ?
                options.getChannelCloseHandler() : this.channelCloseHandler;
    }

    public RpcClient rpcClient(String exchange, String routingKey) {
        return new RpcClient(connectionMono.map(CHANNEL_CREATION_FUNCTION).transform(this::cache), exchange, routingKey);
    }

    public RpcClient rpcClient(String exchange, String routingKey, Supplier<String> correlationIdProvider) {
        return new RpcClient(connectionMono.map(CHANNEL_CREATION_FUNCTION).transform(this::cache), exchange, routingKey, correlationIdProvider);
    }

    /**
     * Declare a queue following the specification.
     *
     * @param specification the specification of the queue
     * @return a mono wrapping the result of the declaration
     * @see QueueSpecification
     */
    public Mono<AMQP.Queue.DeclareOk> declare(QueueSpecification specification) {
        return this.declareQueue(specification, null);
    }

    /**
     * Declare a queue following the specification and the resource management options.
     *
     * @param specification the specification of the queue
     * @param options       options for resource management
     * @return a mono wrapping the result of the declaration
     * @see QueueSpecification
     * @see ResourceManagementOptions
     */
    public Mono<AMQP.Queue.DeclareOk> declare(QueueSpecification specification, ResourceManagementOptions options) {
        return this.declareQueue(specification, options);
    }

    /**
     * Declare a queue following the specification.
     *
     * @param specification the specification of the queue
     * @return a mono wrapping the result of the declaration
     * @see QueueSpecification
     */
    public Mono<AMQP.Queue.DeclareOk> declareQueue(QueueSpecification specification) {
        return this.declareQueue(specification, null);
    }

    /**
     * Declare a queue following the specification and the resource management options.
     *
     * @param specification the specification of the queue
     * @param options       options for resource management
     * @return a mono wrapping the result of the declaration
     * @see QueueSpecification
     * @see ResourceManagementOptions
     */
    public Mono<AMQP.Queue.DeclareOk> declareQueue(QueueSpecification specification, ResourceManagementOptions options) {
        Mono<? extends Channel> channelMono = getChannelMonoForResourceManagement(options);

        AMQP.Queue.Declare declare;
        if (specification.getName() == null) {
            declare = new AMQImpl.Queue.Declare.Builder()
                .queue("")
                .durable(false)
                .exclusive(true)
                .autoDelete(true)
                .arguments(specification.getArguments())
                .build();
        } else {
            declare = new AMQImpl.Queue.Declare.Builder()
                .queue(specification.getName())
                .durable(specification.isDurable())
                .exclusive(specification.isExclusive())
                .autoDelete(specification.isAutoDelete())
                .passive(specification.isPassive())
                .arguments(specification.getArguments())
                .build();
        }

        return channelMono.map(channel -> {
            try {
                return channel.asyncCompletableRpc(declare);
            } catch (IOException e) {
                throw new RabbitFluxException("Error during RPC call", e);
            }
        }).flatMap(future -> Mono.fromCompletionStage(future))
            .flatMap(command -> Mono.just((AMQP.Queue.DeclareOk) command.getMethod()))
            .publishOn(resourceManagementScheduler);
    }

    private Mono<? extends Channel> getChannelMonoForResourceManagement(ResourceManagementOptions options) {
        return options != null && options.getChannelMono() != null ?
            options.getChannelMono() : this.resourceManagementChannelMono;
    }

    public Mono<AMQP.Queue.DeleteOk> delete(QueueSpecification specification) {
        return this.delete(specification, false, false);
    }

    public Mono<AMQP.Queue.DeleteOk> delete(QueueSpecification specification, ResourceManagementOptions options) {
        return this.delete(specification, false, false, options);
    }

    public Mono<AMQP.Queue.DeleteOk> delete(QueueSpecification specification, boolean ifUnused, boolean ifEmpty) {
        return this.deleteQueue(specification, ifUnused, ifEmpty);
    }

    public Mono<AMQP.Queue.DeleteOk> delete(QueueSpecification specification, boolean ifUnused, boolean ifEmpty, ResourceManagementOptions options) {
        return this.deleteQueue(specification, ifUnused, ifEmpty, options);
    }

    public Mono<AMQP.Queue.DeleteOk> deleteQueue(QueueSpecification specification, boolean ifUnused, boolean ifEmpty) {
        return this.deleteQueue(specification, ifUnused, ifEmpty, null);
    }

    public Mono<AMQP.Queue.DeleteOk> deleteQueue(QueueSpecification specification, boolean ifUnused, boolean ifEmpty, ResourceManagementOptions options) {
        Mono<? extends Channel> channelMono = getChannelMonoForResourceManagement(options);
        AMQP.Queue.Delete delete = new AMQImpl.Queue.Delete.Builder()
            .queue(specification.getName())
            .ifUnused(ifUnused)
            .ifEmpty(ifEmpty)
            .build();

        return channelMono.map(channel -> {
            try {
                return channel.asyncCompletableRpc(delete);
            } catch (IOException e) {
                throw new RabbitFluxException("Error during RPC call", e);
            }
        }).flatMap(future -> Mono.fromCompletionStage(future))
            .flatMap(command -> Mono.just((AMQP.Queue.DeleteOk) command.getMethod()))
            .publishOn(resourceManagementScheduler);
    }

    public Mono<AMQP.Exchange.DeclareOk> declare(ExchangeSpecification specification) {
        return this.declareExchange(specification, null);
    }

    public Mono<AMQP.Exchange.DeclareOk> declare(ExchangeSpecification specification, ResourceManagementOptions options) {
        return this.declareExchange(specification, options);
    }

    public Mono<AMQP.Exchange.DeclareOk> declareExchange(ExchangeSpecification specification) {
        return this.declareExchange(specification, null);
    }

    public Mono<AMQP.Exchange.DeclareOk> declareExchange(ExchangeSpecification specification, ResourceManagementOptions options) {
        Mono<? extends Channel> channelMono = getChannelMonoForResourceManagement(options);
        AMQP.Exchange.Declare declare = new AMQImpl.Exchange.Declare.Builder()
            .exchange(specification.getName())
            .type(specification.getType())
            .durable(specification.isDurable())
            .autoDelete(specification.isAutoDelete())
            .internal(specification.isInternal())
            .passive(specification.isPassive())
            .arguments(specification.getArguments())
            .build();
        return channelMono.map(channel -> {
            try {
                return channel.asyncCompletableRpc(declare);
            } catch (IOException e) {
                throw new RabbitFluxException("Error during RPC call", e);
            }
        }).flatMap(future -> Mono.fromCompletionStage(future))
            .flatMap(command -> Mono.just((AMQP.Exchange.DeclareOk) command.getMethod()))
            .publishOn(resourceManagementScheduler);
    }

    public Mono<AMQP.Exchange.DeleteOk> delete(ExchangeSpecification specification) {
        return this.delete(specification, false);
    }

    public Mono<AMQP.Exchange.DeleteOk> delete(ExchangeSpecification specification, ResourceManagementOptions options) {
        return this.delete(specification, false, options);
    }

    public Mono<AMQP.Exchange.DeleteOk> delete(ExchangeSpecification specification, boolean ifUnused) {
        return this.deleteExchange(specification, ifUnused);
    }

    public Mono<AMQP.Exchange.DeleteOk> delete(ExchangeSpecification specification, boolean ifUnused, ResourceManagementOptions options) {
        return this.deleteExchange(specification, ifUnused, options);
    }

    public Mono<AMQP.Exchange.DeleteOk> deleteExchange(ExchangeSpecification specification, boolean ifUnused) {
        return this.deleteExchange(specification, ifUnused, null);
    }

    public Mono<AMQP.Exchange.DeleteOk> deleteExchange(ExchangeSpecification specification, boolean ifUnused, ResourceManagementOptions options) {
        Mono<? extends Channel> channelMono = getChannelMonoForResourceManagement(options);
        AMQP.Exchange.Delete delete = new AMQImpl.Exchange.Delete.Builder()
            .exchange(specification.getName())
            .ifUnused(ifUnused)
            .build();
        return channelMono.map(channel -> {
            try {
                return channel.asyncCompletableRpc(delete);
            } catch (IOException e) {
                throw new RabbitFluxException("Error during RPC call", e);
            }
        }).flatMap(future -> Mono.fromCompletionStage(future))
            .flatMap(command -> Mono.just((AMQP.Exchange.DeleteOk) command.getMethod()))
            .publishOn(resourceManagementScheduler);
    }

    public Mono<AMQP.Queue.UnbindOk> unbind(BindingSpecification specification) {
        return this.unbind(specification, null);
    }

    public Mono<AMQP.Queue.UnbindOk> unbind(BindingSpecification specification, ResourceManagementOptions options) {
        Mono<? extends Channel> channelMono = getChannelMonoForResourceManagement(options);
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
                throw new RabbitFluxException("Error during RPC call", e);
            }
        }).flatMap(future -> Mono.fromCompletionStage(future))
            .flatMap(command -> Mono.just((AMQP.Queue.UnbindOk) command.getMethod()))
            .publishOn(resourceManagementScheduler);
    }

    public Mono<AMQP.Queue.BindOk> bind(BindingSpecification specification) {
        return this.bind(specification, null);
    }

    public Mono<AMQP.Queue.BindOk> bind(BindingSpecification specification, ResourceManagementOptions options) {
        Mono<? extends Channel> channelMono = getChannelMonoForResourceManagement(options);
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
                throw new RabbitFluxException("Error during RPC call", e);
            }
        }).flatMap(future -> Mono.fromCompletionStage(future))
            .flatMap(command -> Mono.just((AMQP.Queue.BindOk) command.getMethod()))
            .publishOn(resourceManagementScheduler);
    }

    public void close() {
        if (closingOrClosed.compareAndSet(false, true)) {
            if (connection.get() != null) {
                safelyExecute(
                        LOGGER,
                        () -> connection.get().close(this.connectionClosingTimeout),
                        "Error while closing sender connection"
                );
            }

            if (this.privateConnectionSubscriptionScheduler) {
                safelyExecute(
                        LOGGER,
                        () -> this.connectionSubscriptionScheduler.dispose(),
                        "Error while disposing connection subscription scheduler"
                );
            }
            if (this.privateResourceManagementScheduler) {
                safelyExecute(
                        LOGGER,
                        () -> this.resourceManagementScheduler.dispose(),
                        "Error while disposing resource management scheduler"
                );
            }
            safelyExecute(
                    LOGGER,
                    () -> channelCloseThreadPool.shutdown(),
                    "Error while closing channel closing thread pool"
            );
        }
    }

    public static class SendContext<T> {

        protected final Channel channel;
        protected final OutboundMessage<T> message;

        protected SendContext(Channel channel, OutboundMessage<T> message) {
            this.channel = channel;
            this.message = message;
        }

        public OutboundMessage<T> getMessage() {
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

    public static class ConfirmSendContext<T> extends SendContext<T> {

        private final PublishConfirmSubscriber<T> subscriber;


        protected ConfirmSendContext(Channel channel, OutboundMessage<T> message, PublishConfirmSubscriber<T> subscriber) {
            super(channel, message);
            this.subscriber = subscriber;

        }

        @Override
        public void publish(OutboundMessage outboundMessage) throws Exception {
            long nextPublishSeqNo = channel.getNextPublishSeqNo();
            try {
                subscriber.unconfirmed.putIfAbsent(nextPublishSeqNo, this.message);
                this.channel.basicPublish(
                        outboundMessage.getExchange(),
                        outboundMessage.getRoutingKey(),
                        this.subscriber.trackReturned, // this happens to be the same value as the mandatory flag
                        this.subscriber.propertiesProcessor.apply(message.getProperties(), nextPublishSeqNo),
                        outboundMessage.getBody()
                );
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

    private static class PublishConfirmOperator<T>
        extends FluxOperator<OutboundMessage<T>, OutboundMessageResult<T>> {

        private final Channel channel;

        private final SendOptions options;

        public PublishConfirmOperator(Publisher<OutboundMessage<T>> source, Channel channel, SendOptions options) {
            super(Flux.from(source));
            this.channel = channel;
            this.options = options;
        }

        @Override
        public void subscribe(CoreSubscriber<? super OutboundMessageResult<T>> actual) {
            source.subscribe(new PublishConfirmSubscriber<>(channel, actual, options));
        }
    }

    private static class PublishConfirmSubscriber<T> implements
        CoreSubscriber<OutboundMessage<T>>, Subscription {

        private final AtomicReference<SubscriberState> state = new AtomicReference<>(SubscriberState.INIT);

        private final AtomicReference<Throwable> firstException = new AtomicReference<>();

        private final ConcurrentNavigableMap<Long, OutboundMessage<T>> unconfirmed = new ConcurrentSkipListMap<>();

        private final Channel channel;

        private final Subscriber<? super OutboundMessageResult<T>> subscriber;

        private final BiConsumer<SendContext, Exception> exceptionHandler;

        private Subscription subscription;

        private ConfirmListener confirmListener;

        private ReturnListener returnListener;

        private final boolean trackReturned;

        private final BiFunction<AMQP.BasicProperties, Long, AMQP.BasicProperties> propertiesProcessor;

        private PublishConfirmSubscriber(Channel channel, Subscriber<? super OutboundMessageResult<T>> subscriber, SendOptions options) {
            this.channel = channel;
            this.subscriber = subscriber;
            this.exceptionHandler = options.getExceptionHandler();
            this.trackReturned = options.isTrackReturned();
            if (this.trackReturned) {
                this.propertiesProcessor = PublishConfirmSubscriber::addReactorRabbitMQDeliveryTag;
            } else {
                this.propertiesProcessor = (properties, deliveryTag) -> properties;
            }
        }

        @Override
        public void request(long n) {
            subscription.request(n);
        }

        @Override
        public void cancel() {
            subscription.cancel();
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            if (Operators.validate(this.subscription, subscription)) {

                if(this.trackReturned) {
                    this.returnListener = (replyCode, replyText, exchange, routingKey, properties, body) -> {
                        try {
                            Object deliveryTagObj = properties.getHeaders().get(REACTOR_RABBITMQ_DELIVERY_TAG_HEADER);
                            if(deliveryTagObj instanceof Long) {
                                Long deliveryTag = (Long) deliveryTagObj;
                                OutboundMessage<T> outboundMessage = unconfirmed.get(deliveryTag);
                                subscriber.onNext(new OutboundMessageResult<>(outboundMessage, true, true));
                                unconfirmed.remove(deliveryTag);
                            } else {
                                handleError(new IllegalArgumentException("Missing header " + REACTOR_RABBITMQ_DELIVERY_TAG_HEADER), null);
                            }
                        } catch (Exception e) {
                            handleError(e, null);
                        }
                    };
                    channel.addReturnListener(this.returnListener);
                }


                this.confirmListener = new ConfirmListener() {

                    @Override
                    public void handleAck(long deliveryTag, boolean multiple) {
                        handleAckNack(deliveryTag, multiple, true);
                    }

                    @Override
                    public void handleNack(long deliveryTag, boolean multiple) {
                        handleAckNack(deliveryTag, multiple, false);
                    }

                    private void handleAckNack(long deliveryTag, boolean multiple, boolean ack) {
                        if (multiple) {
                            try {
                                ConcurrentNavigableMap<Long, OutboundMessage<T>> unconfirmedToSend = unconfirmed.headMap(deliveryTag, true);
                                Iterator<Map.Entry<Long, OutboundMessage<T>>> iterator = unconfirmedToSend.entrySet().iterator();
                                while (iterator.hasNext()) {
                                    subscriber.onNext(new OutboundMessageResult<>(iterator.next().getValue(), ack, false));
                                    iterator.remove();
                                }
                            } catch (Exception e) {
                                handleError(e, null);
                            }
                        } else {
                            OutboundMessage<T> outboundMessage = unconfirmed.get(deliveryTag);
                            if(outboundMessage != null) {
                                try {
                                    unconfirmed.remove(deliveryTag);
                                    subscriber.onNext(new OutboundMessageResult<>(outboundMessage, ack, false));
                                } catch (Exception e) {
                                    handleError(e, new OutboundMessageResult<>(outboundMessage, ack, false));
                                }
                            }
                        }
                        if (unconfirmed.isEmpty()) {
                            maybeComplete();
                        }
                    }
                };
                channel.addConfirmListener(confirmListener);
                channel.addShutdownListener(sse -> {
                    // the server is closing the channel because of some error (e.g. exchange does not exist).
                    // sending a signal downstream
                    if (!sse.isHardError() && !sse.isInitiatedByApplication()) {
                        subscriber.onError(sse);
                    }
                });
                state.set(SubscriberState.ACTIVE);
                this.subscription = subscription;
                subscriber.onSubscribe(this);
            }
        }

        @Override
        public void onNext(OutboundMessage<T> message) {
            if (checkComplete(message)) {
                return;
            }

            long nextPublishSeqNo = channel.getNextPublishSeqNo();
            try {
                unconfirmed.putIfAbsent(nextPublishSeqNo, message);
                channel.basicPublish(
                        message.getExchange(),
                        message.getRoutingKey(),
                        this.trackReturned, // this happens to be the same value as the mandatory flag
                        this.propertiesProcessor.apply(message.getProperties(), nextPublishSeqNo),
                        message.getBody()
                );
            } catch (Exception e) {
                unconfirmed.remove(nextPublishSeqNo);
                try {
                    this.exceptionHandler.accept(new ConfirmSendContext<>(channel, message, this), e);
                } catch (Exception innerException) {
                    handleError(innerException, new OutboundMessageResult<>(message, false, false));
                }
            }
        }

        private static AMQP.BasicProperties addReactorRabbitMQDeliveryTag(AMQP.BasicProperties properties, long deliveryTag) {
            AMQP.BasicProperties baseProperties = properties != null ? properties : new AMQP.BasicProperties();

            Map<String, Object> headers = baseProperties.getHeaders() != null ? new HashMap<>(baseProperties.getHeaders()) : new HashMap<>();

            headers.putIfAbsent(REACTOR_RABBITMQ_DELIVERY_TAG_HEADER, deliveryTag);

            return baseProperties.builder().headers(headers).build();
        }

        @Override
        public void onError(Throwable throwable) {
            if (state.compareAndSet(SubscriberState.ACTIVE, SubscriberState.COMPLETE) ||
                state.compareAndSet(SubscriberState.OUTBOUND_DONE, SubscriberState.COMPLETE)) {
                // complete the flux state
                channel.removeConfirmListener(confirmListener);

                if (returnListener != null) {
                    channel.removeReturnListener(returnListener);
                }

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

        private void handleError(Exception e, OutboundMessageResult<T> result) {
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
                channel.removeConfirmListener(confirmListener);

                if (returnListener != null) {
                    channel.removeReturnListener(returnListener);
                }

                subscriber.onComplete();
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
                throw new RabbitFluxException("Error while creating channel", e);
            }
        }
    }

    private static class ChannelProxyCreationFunction implements Function<Connection, Channel> {

        @Override
        public Channel apply(Connection connection) {
            try {
                return new ChannelProxy(connection);
            } catch (IOException e) {
                throw new RabbitFluxException("Error while creating channel", e);
            }
        }
    }
}
