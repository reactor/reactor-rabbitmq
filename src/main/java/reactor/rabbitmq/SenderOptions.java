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
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Options for {@link Sender} creation.
 */
public class SenderOptions {

    private ConnectionFactory connectionFactory = ((Supplier<ConnectionFactory>) () -> {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        return connectionFactory;
    }).get();

    private Mono<? extends Connection> connectionMono;

    /**
     * Channel mono used in sending methods.
     *
     * @since 1.1.0
     */
    private Mono<? extends Channel> channelMono;

    /**
     * Logic to close channels.
     *
     * @see ChannelCloseHandlers.SenderChannelCloseHandler
     * @since 1.1.0
     */
    private BiConsumer<SignalType, Channel> channelCloseHandler;

    private Scheduler resourceManagementScheduler;

    private Scheduler connectionSubscriptionScheduler;

    private Mono<? extends Channel> resourceManagementChannelMono;

    private Utils.ExceptionFunction<ConnectionFactory, ? extends Connection> connectionSupplier;

    private Function<Mono<? extends Connection>, Mono<? extends Connection>> connectionMonoConfigurator = cm -> cm;

    /**
     * Timeout for closing the {@link Sender} connection.
     * <p>
     * Default is 30 seconds. Use {@link Duration#ZERO} for no timeout.
     *
     * @since 1.3.0
     */
    private Duration connectionClosingTimeout = Duration.ofSeconds(30);

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public SenderOptions connectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        return this;
    }

    @Nullable
    public Scheduler getResourceManagementScheduler() {
        return resourceManagementScheduler;
    }

    /**
     * Resource management scheduler.
     * It is developer's responsibility to close it if set.
     *
     * @param resourceManagementScheduler
     * @return the current {@link SenderOptions} instance
     */
    public SenderOptions resourceManagementScheduler(@Nullable Scheduler resourceManagementScheduler) {
        this.resourceManagementScheduler = resourceManagementScheduler;
        return this;
    }

    @Nullable
    public Scheduler getConnectionSubscriptionScheduler() {
        return connectionSubscriptionScheduler;
    }

    /**
     * Scheduler used on connection creation subscription.
     * It is developer's responsibility to close it if set.
     *
     * @param connectionSubscriptionScheduler
     * @return the current {@link SenderOptions} instance
     */
    public SenderOptions connectionSubscriptionScheduler(@Nullable Scheduler connectionSubscriptionScheduler) {
        this.connectionSubscriptionScheduler = connectionSubscriptionScheduler;
        return this;
    }

    /**
     * Set a callback that will be passed in the {@link ConnectionFactory} of this {@link SenderOptions} instance to create a {@link Connection}.
     * <p>
     * Note the created connection will be used by a {@link Sender} instance, which will cache it for re-use in its operations
     * and close it when {@link Sender#close()} is called.
     *
     * @param function callback to create a {@link Connection}
     * @return this current {@link SenderOptions} instance
     */
    public SenderOptions connectionSupplier(Utils.ExceptionFunction<ConnectionFactory, ? extends Connection> function) {
        return this.connectionSupplier(this.connectionFactory, function);
    }

    /**
     * Set a callback that will be passed in the given {@link ConnectionFactory} to create a {@link Connection}.
     * <p>
     * Note the created connection will be used by a {@link Sender} instance, which will cache it for re-use in its operations
     * and close it when {@link Sender#close()} is called.
     *
     * @param connectionFactory the {@link ConnectionFactory} passed-in to the creation function
     * @param function          callback to create a {@link Connection}
     * @return this current {@link SenderOptions}
     */
    public SenderOptions connectionSupplier(ConnectionFactory connectionFactory, Utils.ExceptionFunction<ConnectionFactory, ? extends Connection> function) {
        this.connectionSupplier = ignored -> function.apply(connectionFactory);
        return this;
    }

    /**
     * Set a {@link Mono} that the created {@link Sender} will use for its operations.
     * <p>
     * A {@link Sender} created from this {@link SenderOptions} instance will not cache for re-use, nor close
     * on {@link Sender#close()} the underlying connection. It is recommended that the passed-in {@link Mono} handles
     * caching of some sort to avoid a new connection to be created every time the {@link Sender} does an operation.
     * It is the developer's responsibility to close the underlying {@link Connection} once the {@link Sender} is closed
     * and no longer needs it.
     *
     * @param connectionMono
     * @return this current {@link SenderOptions}
     */
    public SenderOptions connectionMono(@Nullable Mono<? extends Connection> connectionMono) {
        this.connectionMono = connectionMono;
        return this;
    }

    @Nullable
    public Mono<? extends Connection> getConnectionMono() {
        return connectionMono;
    }

    /**
     * Sets the channel mono to use in send methods.
     *
     * @param channelMono the channel mono to use
     * @return this {@link SenderOptions} instance
     * @since 1.1.0
     */
    public SenderOptions channelMono(@Nullable Mono<? extends Channel> channelMono) {
        this.channelMono = channelMono;
        return this;
    }

    /**
     * Returns the channel mono to use in send methods.
     *
     * @return the channel mono to use
     * @since 1.1.0
     */
    @Nullable
    public Mono<? extends Channel> getChannelMono() {
        return channelMono;
    }

    /**
     * Returns the channel closing logic.
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
     * @param channelCloseHandler the closing logic
     * @return this {@link SenderOptions} instance
     * @since 1.1.0
     */
    public SenderOptions channelCloseHandler(@Nullable BiConsumer<SignalType, Channel> channelCloseHandler) {
        this.channelCloseHandler = channelCloseHandler;
        return this;
    }

    /**
     * Set the channel pool to use to send messages.
     *
     * @param channelPool
     * @return this {@link SenderOptions} instance
     * @since 1.1.0
     */
    public SenderOptions channelPool(ChannelPool channelPool) {
        this.channelMono = channelPool.getChannelMono();
        this.channelCloseHandler = channelPool.getChannelCloseHandler();
        return this;
    }

    /**
     * A {@link Function} to customize the connection {@link Mono} used in the created {@link Sender} instance.
     * <p>
     * This function can be used to configure retry when obtaining the {@link Connection}.
     *
     * <em>This function is not applied if a custom <code>connectionMono</code> is provided.</em> It is applied
     * when a <code>connectionSupplier</code> is provided though.
     *
     * @param connectionMonoConfigurator the function to configure the passed-in connection mono
     * @return the configured connection mono
     */
    public SenderOptions connectionMonoConfigurator(Function<Mono<? extends Connection>, Mono<? extends Connection>> connectionMonoConfigurator) {
        this.connectionMonoConfigurator = connectionMonoConfigurator;
        return this;
    }

    public SenderOptions resourceManagementChannelMono(@Nullable Mono<? extends Channel> resourceManagementChannelMono) {
        this.resourceManagementChannelMono = resourceManagementChannelMono;
        return this;
    }

    @Nullable
    public Mono<? extends Channel> getResourceManagementChannelMono() {
        return resourceManagementChannelMono;
    }

    @Nullable
    public Utils.ExceptionFunction<ConnectionFactory, ? extends Connection> getConnectionSupplier() {
        return connectionSupplier;
    }

    public Function<Mono<? extends Connection>, Mono<? extends Connection>> getConnectionMonoConfigurator() {
        return connectionMonoConfigurator;
    }

    /**
     * Timeout for closing the {@link Sender} connection.
     * <p>
     * Default is 30 seconds. Use {@link Duration#ZERO} for no timeout.
     *
     * @param connectionClosingTimeout timeout for connection closing
     * @return this {@link SenderOptions} instance
     * @since 1.3.0
     */
    public SenderOptions connectionClosingTimeout(@Nullable Duration connectionClosingTimeout) {
        this.connectionClosingTimeout = connectionClosingTimeout;
        return this;
    }

    @Nullable
    public Duration getConnectionClosingTimeout() {
        return connectionClosingTimeout;
    }
}
