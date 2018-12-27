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

import com.rabbitmq.client.Channel;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.time.Duration;
import java.util.function.BiConsumer;

/**
 * Options for {@link Sender}#send* methods.
 */
public class SendOptions {

    private BiConsumer<Sender.SendContext, Exception> exceptionHandler = new ExceptionHandlers.RetrySendingExceptionHandler(
        Duration.ofSeconds(10), Duration.ofMillis(200), ExceptionHandlers.CONNECTION_RECOVERY_PREDICATE
    );

    private Mono<? extends Channel> channelMono;

    private BiConsumer<SignalType, Channel> channelCloseHandler;

    public BiConsumer<Sender.SendContext, Exception> getExceptionHandler() {
        return exceptionHandler;
    }

    public SendOptions exceptionHandler(BiConsumer<Sender.SendContext, Exception> exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
        return this;
    }

    public Mono<? extends Channel> getChannelMono() {
        return channelMono;
    }

    public SendOptions channelMono(Mono<? extends Channel> channelMono) {
        this.channelMono = channelMono;
        return this;
    }

    public BiConsumer<SignalType, Channel> getChannelCloseHandler() {
        return channelCloseHandler;
    }

    public SendOptions channelCloseHandler(BiConsumer<SignalType, Channel> channelCloseHandler) {
        this.channelCloseHandler = channelCloseHandler;
        return this;
    }
}
