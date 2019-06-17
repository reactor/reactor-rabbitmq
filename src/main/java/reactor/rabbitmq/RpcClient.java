/*
 * Copyright (c) 2018-2019 Pivotal Software Inc, All Rights Reserved.
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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoOperator;
import reactor.core.publisher.Operators;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 *
 */
public class RpcClient implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcClient.class);

    private final Mono<Channel> channelMono;

    private final String exchange;

    private final String routingKey;

    private final String replyTo = "amq.rabbitmq.reply-to";

    private final AtomicBoolean consumerSetUp = new AtomicBoolean(false);

    private final ConcurrentMap<String, RpcSubscriber> subscribers = new ConcurrentHashMap<>();

    private final AtomicReference<String> consumerTag = new AtomicReference<>();

    private final Supplier<String> correlationIdSupplier;

    private final Lock channelLock = new ReentrantLock();

    public RpcClient(Mono<Channel> channelMono, String exchange, String routingKey, Supplier<String> correlationIdSupplier) {
        this.channelMono = channelMono;
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.correlationIdSupplier = correlationIdSupplier;
    }

    public RpcClient(Mono<Channel> channelMono, String exchange, String routingKey) {
        this(channelMono, exchange, routingKey, defaultCorrelationProvider());
    }

    private static final Supplier<String> defaultCorrelationProvider() {
        final AtomicLong correlationId = new AtomicLong(0);
        return () -> String.valueOf(correlationId.getAndIncrement());
    }

    public Mono<Delivery> rpc(Publisher<RpcRequest> request) {
        return channelMono.flatMap(channel -> new RpcOperator(request, channel));
    }

    public void close() {
        if (!subscribers.isEmpty()) {
            LOGGER.warn("Closing RPC client with outstanding request(s): " + subscribers.keySet());
        }
        if (consumerTag.get() != null) {
            try {
                Channel channel = this.channelMono.block();
                lockChannel();
                try {
                    channel.basicCancel(consumerTag.get());
                } finally {
                    unlockChannel();
                }
            } catch (IOException e) {
                throw new RabbitFluxException(e);
            }
        }
    }

    protected void lockChannel() {
        channelLock.lock();
    }

    protected void unlockChannel() {
        channelLock.unlock();
    }

    public static class RpcRequest {

        private final AMQP.BasicProperties properties;
        private final byte[] body;

        public RpcRequest(AMQP.BasicProperties properties, byte[] body) {
            this.properties = properties;
            this.body = body;
        }

        public RpcRequest(byte[] body) {
            this(null, body);
        }
    }

    private class RpcSubscriber implements CoreSubscriber<RpcRequest> {

        private final AtomicReference<SubscriberState> state = new AtomicReference<>(SubscriberState.INIT);

        private final AtomicReference<Throwable> firstException = new AtomicReference<Throwable>();

        private final Channel channel;

        private final Subscriber<? super Delivery> subscriber;

        private final AtomicBoolean received = new AtomicBoolean(false);

        RpcSubscriber(Channel channel, Subscriber<? super Delivery> subscriber) {
            this.channel = channel;
            this.subscriber = subscriber;
        }

        @Override
        public void onSubscribe(Subscription s) {
            // FIXME set up global consumer outside of a given call
            if (consumerSetUp.compareAndSet(false, true)) {
                DeliverCallback deliver = (consumerTag, delivery) -> {
                    String correlationId = delivery.getProperties().getCorrelationId();
                    RpcSubscriber rpcSubscriber = subscribers.remove(correlationId);
                    if (rpcSubscriber == null) {
                        throw new RabbitFluxException("No outstanding request for correlation ID " + correlationId);
                    } else {
                        rpcSubscriber.subscriber.onNext(delivery);
                        rpcSubscriber.received.set(true);
                    }
                };
                try {
                    lockChannel();
                    try {
                        String ctag = channel.basicConsume(replyTo, true, deliver, consumerTag -> {
                        });
                        consumerTag.set(ctag);
                    } finally {
                        unlockChannel();
                    }
                } catch (IOException e) {
                    handleError(e);
                }
            }
            state.set(SubscriberState.ACTIVE);
            this.subscriber.onSubscribe(s);
        }

        @Override
        public void onNext(RpcRequest request) {
            if (checkComplete(request)) {
                return;
            }
            try {
                String correlationId = correlationIdSupplier.get();
                AMQP.BasicProperties properties = request.properties;
                properties = ((properties == null) ? new AMQP.BasicProperties.Builder() : properties.builder())
                        .correlationId(correlationId).replyTo(replyTo).build();
                subscribers.put(correlationId, this);
                lockChannel();
                try {
                    channel.basicPublish(exchange, routingKey, properties, request.body);
                } finally {
                    unlockChannel();
                }
            } catch (IOException e) {
                handleError(e);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (state.compareAndSet(SubscriberState.ACTIVE, SubscriberState.COMPLETE) ||
                    state.compareAndSet(SubscriberState.OUTBOUND_DONE, SubscriberState.COMPLETE)) {
                // complete the state
                subscriber.onError(throwable);
            } else if (firstException.compareAndSet(null, throwable) && state.get() == SubscriberState.COMPLETE) {
                // already completed, drop the error
                Operators.onErrorDropped(throwable, currentContext());
            }
        }

        @Override
        public void onComplete() {
            if (state.compareAndSet(SubscriberState.ACTIVE, SubscriberState.OUTBOUND_DONE) && received.get()) {
                maybeComplete();
            }
        }

        private void maybeComplete() {
            boolean done = state.compareAndSet(SubscriberState.OUTBOUND_DONE, SubscriberState.COMPLETE);
            if (done) {
                subscriber.onComplete();
            }
        }

        private void handleError(Exception e) {
            LOGGER.error("error during RPC", e);
            boolean complete = checkComplete(e);
            firstException.compareAndSet(null, e);
            if (!complete) {
                onError(e);
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

    private class RpcOperator extends MonoOperator<RpcRequest, Delivery> {

        private final Channel channel;

        protected RpcOperator(Publisher<RpcRequest> source, Channel channel) {
            super(Mono.from(source));
            this.channel = channel;
        }

        @Override
        public void subscribe(CoreSubscriber<? super Delivery> actual) {
            source.subscribe(new RpcSubscriber(channel, actual));
        }
    }
}
