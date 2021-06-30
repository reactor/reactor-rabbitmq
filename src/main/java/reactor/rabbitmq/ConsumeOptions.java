/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Delivery;
import reactor.core.publisher.FluxSink;

import java.time.Duration;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Options for {@link Receiver}#consume* methods.
 */
public class ConsumeOptions {

    /**
     * Quality of Service (prefetch count) when using acknowledgment.
     * Default is 250.
     */
    private int qos = 250;

    /**
     * A client-generated consumer tag to establish context
     */
    private String consumerTag = "";

    private FluxSink.OverflowStrategy overflowStrategy = FluxSink.OverflowStrategy.BUFFER;

    /**
     * whether the message should be emitted downstream or not
     */
    private BiFunction<Long, ? super Delivery, Boolean> hookBeforeEmitBiFunction = (requestedFromDownstream, message) -> true;

    /**
     * whether the flux should be completed after the emission of the message
     */
    private BiFunction<Long, ? super Delivery, Boolean> stopConsumingBiFunction = (requestedFromDownstream, message) -> false;

    private BiConsumer<Receiver.AcknowledgmentContext, Exception> exceptionHandler = new ExceptionHandlers.RetryAcknowledgmentExceptionHandler(
            Duration.ofSeconds(10), Duration.ofMillis(200), ExceptionHandlers.CONNECTION_RECOVERY_PREDICATE
    );

    private Consumer<Channel> channelCallback = ch -> {
    };

    public int getQos() {
        return qos;
    }

    public ConsumeOptions qos(int qos) {
        if (qos < 0) {
            throw new IllegalArgumentException("QoS must be greater or equal to 0");
        }
        this.qos = qos;
        return this;
    }

    public String getConsumerTag() {
        return consumerTag;
    }

    public ConsumeOptions consumerTag(String consumerTag) {
        if (consumerTag == null) {
            throw new IllegalArgumentException("consumerTag must be non-null");
        }
        this.consumerTag = consumerTag;
        return this;
    }

    public FluxSink.OverflowStrategy getOverflowStrategy() {
        return overflowStrategy;
    }

    public ConsumeOptions overflowStrategy(FluxSink.OverflowStrategy overflowStrategy) {
        this.overflowStrategy = overflowStrategy;
        return this;
    }

    public BiFunction<Long, ? super Delivery, Boolean> getHookBeforeEmitBiFunction() {
        return hookBeforeEmitBiFunction;
    }

    public ConsumeOptions hookBeforeEmitBiFunction(BiFunction<Long, ? super Delivery, Boolean> hookBeforeEmit) {
        this.hookBeforeEmitBiFunction = hookBeforeEmit;
        return this;
    }

    public BiFunction<Long, ? super Delivery, Boolean> getStopConsumingBiFunction() {
        return stopConsumingBiFunction;
    }

    public ConsumeOptions stopConsumingBiFunction(
            BiFunction<Long, ? super Delivery, Boolean> stopConsumingBiFunction) {
        this.stopConsumingBiFunction = stopConsumingBiFunction;
        return this;
    }

    public ConsumeOptions exceptionHandler(BiConsumer<Receiver.AcknowledgmentContext, Exception> exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
        return this;
    }

    public BiConsumer<Receiver.AcknowledgmentContext, Exception> getExceptionHandler() {
        return exceptionHandler;
    }

    public ConsumeOptions channelCallback(Consumer<Channel> channelCallback) {
        this.channelCallback = channelCallback;
        return this;
    }

    public Consumer<Channel> getChannelCallback() {
        return channelCallback;
    }
}
