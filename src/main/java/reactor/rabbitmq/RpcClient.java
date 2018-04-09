/*
 * Copyright (c) 2018 Pivotal Software Inc, All Rights Reserved.
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

/**
 *
 */
public class RpcClient implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcClient.class);

    private final Mono<Channel> channelMono;

    private final AtomicLong correlationId = new AtomicLong(0);

    private final String exchange;

    private final String routingKey;

    private final String replyTo = "amq.rabbitmq.reply-to";

    private final AtomicBoolean consumerSetUp = new AtomicBoolean(false);

    private final ConcurrentMap<String, RpcSubscriber> subscribers = new ConcurrentHashMap<>();

    private final AtomicReference<String> consumerTag = new AtomicReference<>();

    public RpcClient(Mono<Channel> channelMono, String exchange, String routingKey) {
        this.channelMono = channelMono;
        this.exchange = exchange;
        this.routingKey = routingKey;
    }

    public Mono<Delivery> rpc(Publisher<RpcRequest> request) {
        return channelMono.flatMap(channel -> new RpcOperator(request, channel));
    }

    public void close() {
        if (consumerTag.get() != null) {
            try {
                this.channelMono.block().basicCancel(consumerTag.get());
            } catch (IOException e) {
                throw new ReactorRabbitMqException(e);
            }
        }
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
            // FIXME try to set up global consumer outside of a given call
            if (consumerSetUp.compareAndSet(false, true)) {
                DeliverCallback deliver = (consumerTag, delivery) -> {
                    String correlationId = delivery.getProperties().getCorrelationId();
                    RpcSubscriber rpcSubscriber = subscribers.remove(correlationId);
                    if (rpcSubscriber == null) {
                        // FIXME handle null outstanding RPC
                    } else {
                        rpcSubscriber.subscriber.onNext(delivery);
                        rpcSubscriber.received.set(true);
                    }
                };
                try {
                    String ctag = channel.basicConsume(replyTo, true, deliver, consumerTag -> {
                    });
                    consumerTag.set(ctag);
                } catch (IOException e) {
                    handleError(e, null);
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
                String correlationId = "" + RpcClient.this.correlationId.getAndIncrement();
                AMQP.BasicProperties props = request.properties;
                props = ((props == null) ? new AMQP.BasicProperties.Builder() : props.builder())
                    .correlationId(correlationId).replyTo(replyTo).build();
                subscribers.put(correlationId, this);
                channel.basicPublish(exchange, routingKey, props, request.body);
            } catch (IOException e) {
                handleError(e, request);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (state.compareAndSet(SubscriberState.ACTIVE, SubscriberState.COMPLETE) ||
                state.compareAndSet(SubscriberState.OUTBOUND_DONE, SubscriberState.COMPLETE)) {
                closeResources();
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

        private void handleError(Exception e, RpcRequest request) {
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
