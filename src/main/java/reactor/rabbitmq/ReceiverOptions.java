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

import com.rabbitmq.client.Delivery;
import reactor.core.publisher.FluxSink;

import java.util.function.BiFunction;

/**
 *
 */
public class ReceiverOptions {

    private int qos = 0;

    private FluxSink.OverflowStrategy overflowStrategy = FluxSink.OverflowStrategy.BUFFER;

    /** whether the message should be emitted downstream or not */
    private BiFunction<FluxSink<?>, ? super Delivery, Boolean> hookBeforeEmit = (fluxSink, message) -> true;

    /** whether the flux should be completed after the emission of the message */
    private BiFunction<FluxSink<?>, Delivery, Boolean> stopConsumingBiFunction = (fluxSink, message) -> false;

    public int getQos() {
        return qos;
    }

    public ReceiverOptions qos(int qos) {
        if(qos < 0) {
            throw new IllegalArgumentException("QoS must greater or equal to 0");
        }
        this.qos = qos;
        return this;
    }

    public FluxSink.OverflowStrategy getOverflowStrategy() {
        return overflowStrategy;
    }

    public ReceiverOptions overflowStrategy(FluxSink.OverflowStrategy overflowStrategy) {
        this.overflowStrategy = overflowStrategy;
        return this;
    }

    public BiFunction<FluxSink<?>, ? super Delivery, Boolean> getHookBeforeEmit() {
        return hookBeforeEmit;
    }

    public ReceiverOptions hookBeforeEmit(BiFunction<FluxSink<?>, ? super Delivery, Boolean> hookBeforeEmit) {
        this.hookBeforeEmit = hookBeforeEmit;
        return this;
    }

    public BiFunction<FluxSink<?>, Delivery, Boolean> getStopConsumingBiFunction() {
        return stopConsumingBiFunction;
    }

    public ReceiverOptions stopConsumingBiFunction(
        BiFunction<FluxSink<?>, Delivery, Boolean> stopConsumingBiFunction) {
        this.stopConsumingBiFunction = stopConsumingBiFunction;
        return this;
    }

}
