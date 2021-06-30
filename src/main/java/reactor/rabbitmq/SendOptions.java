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
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;

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
     * The maximum number of in-flight records that are fetched
     * from the outbound record publisher while publisher confirms are pending.
     * <p>
     * The number of in-flight records is not limited by default.
     *
     * @since 1.2.0
     */
    private Integer maxInFlight;

    /**
     * Whether we should track returned (undeliverable) messages. Default is false.
     */
    private boolean trackReturned;

    /**
     * The scheduler used for publishing publisher confirms when in-flight records are limited.
     * <p>
     * The default is {@link Schedulers#immediate()}, so the caller's thread.
     *
     * @see #maxInFlight
     * @since 1.2.0
     */
    private Scheduler scheduler = Schedulers.immediate();

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

    /**
     * Returns the maximum number of in-flight records that are fetched
     * from the outbound record publisher while publisher confirms are pending.
     *
     * @return maximum number of in-flight records
     * @since 1.2.0
     */
    @Nullable
    public Integer getMaxInFlight() {
        return maxInFlight;
    }

    /**
     * Returns whether we should track returned (undeliverable) messages when publisher confirms are in use.
     *
     * @return whether we should track returned messages
     * @see Sender#sendWithPublishConfirms(Publisher, SendOptions)
     * @see <a href="https://www.rabbitmq.com/publishers.html#unroutable">Mandatory flag</a>
     * @since 1.3.0
     */
    public boolean isTrackReturned() {
        return trackReturned;
    }

    /**
     * Set whether we should track returned (undeliverable) messages when publisher confirms are in use.
     * <p>
     * Default is false.
     *
     * @param trackReturned
     * @return this {@link SendOptions} instance
     * @see Sender#sendWithPublishConfirms(Publisher, SendOptions)
     * @see <a href="https://www.rabbitmq.com/publishers.html#unroutable">Mandatory flag</a>
     * @since 1.3.0
     */
    public SendOptions trackReturned(boolean trackReturned) {
        this.trackReturned = trackReturned;
        return this;
    }

    /**
     * Set the maximum number of in-flight records that are fetched
     * from the outbound record publisher while publisher confirms are pending.
     * <p>
     * The number of in-flight records is not limited by default.
     *
     * @param maxInFlight
     * @return this {@link SendOptions} instance
     * @since 1.2.0
     */
    public SendOptions maxInFlight(int maxInFlight) {
        this.maxInFlight = maxInFlight;
        return this;
    }

    /**
     * Set the maximum number of in-flight records that are fetched
     * from the outbound record publisher while publisher confirms are pending.
     * Results are run on a supplied {@link Scheduler}
     *
     * @param maxInFlight
     * @param scheduler
     * @return this {@link SendOptions} instance
     * @since 1.2.0
     */
    public SendOptions maxInFlight(int maxInFlight, Scheduler scheduler) {
        this.maxInFlight = maxInFlight;
        this.scheduler = scheduler;
        return this;
    }

    /**
     * The scheduler used for publishing send results.
     *
     * @return scheduler used for publishing send results
     * @since 1.2.0
     */
    public Scheduler getScheduler() {
        return scheduler;
    }

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
    @Nullable
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
    public SendOptions channelMono(@Nullable Mono<? extends Channel> channelMono) {
        this.channelMono = channelMono;
        return this;
    }

    /**
     * Return the channel closing logic.
     *
     * @return the closing logic to use
     * @since 1.1.0
     */
    @Nullable
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
    public SendOptions channelCloseHandler(@Nullable BiConsumer<SignalType, Channel> channelCloseHandler) {
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
