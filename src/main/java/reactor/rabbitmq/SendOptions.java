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

    /**
     * Channel mono to use to send messages.
     *
     * @since 1.1.0
     */
    private Mono<? extends Channel> channelMono;

    /**
     * Channel closing logic.
     *
     * @since 1.1.0
     */
    private BiConsumer<SignalType, Channel> channelCloseHandler;

    public BiConsumer<Sender.SendContext, Exception> getExceptionHandler() {
        return exceptionHandler;
    }

    public SendOptions exceptionHandler(BiConsumer<Sender.SendContext, Exception> exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
        return this;
    }

    /**
     * Return the channel mono to use to send messages.
     *
     * @return the channel mono to use
     * @since 1.1.0
     */
    public Mono<? extends Channel> getChannelMono() {
        return channelMono;
    }

    /**
     * Set the channel mono to use to send messages.
     *
     * @param channelMono
     * @return this {@link SendOptions} instance
     * @since 1.1.0
     */
    public SendOptions channelMono(Mono<? extends Channel> channelMono) {
        this.channelMono = channelMono;
        return this;
    }

    /**
     * Return the channel closing logic.
     *
     * @return the closing logic to use
     * @since 1.1.0
     */
    public BiConsumer<SignalType, Channel> getChannelCloseHandler() {
        return channelCloseHandler;
    }

    /**
     * Set the channel closing logic.
     *
     * @param channelCloseHandler
     * @return this {@link SendOptions} instance
     * @since 1.1.0
     */
    public SendOptions channelCloseHandler(BiConsumer<SignalType, Channel> channelCloseHandler) {
        this.channelCloseHandler = channelCloseHandler;
        return this;
    }

    /**
     * Set the channel pool to use to send messages.
     * <p>
     * It is developer's responsibility to close it if set.
     *
     * @param channelPool
     * @return this {@link SendOptions} instance
     * @since 1.1.0
     */
    public SendOptions channelPool(ChannelPool channelPool) {
        this.channelMono = channelPool.getChannelMono();
        this.channelCloseHandler = channelPool.getChannelCloseHandler();
        return this;
    }
}
