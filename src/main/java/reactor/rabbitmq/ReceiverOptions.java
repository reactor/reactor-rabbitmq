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

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.util.function.Supplier;

/**
 * Options for {@link Receiver} creation.
 */
public class ReceiverOptions {

    private ConnectionFactory connectionFactory = ((Supplier<ConnectionFactory>) () -> {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        return connectionFactory;
    }).get();

    private Mono<? extends Connection> connectionMono;

    private Scheduler connectionSubscriptionScheduler;

    private Utils.ExceptionFunction<ConnectionFactory, ? extends Connection> connectionSupplier;

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public ReceiverOptions connectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        return this;
    }

    public Scheduler getConnectionSubscriptionScheduler() {
        return connectionSubscriptionScheduler;
    }

    /**
     * Scheduler used on connection creation subscription.
     * It is developer's responsibility to close it if set.
     *
     * @param connectionSubscriptionScheduler
     * @return the current {@link ReceiverOptions} instance
     */
    public ReceiverOptions connectionSubscriptionScheduler(Scheduler connectionSubscriptionScheduler) {
        this.connectionSubscriptionScheduler = connectionSubscriptionScheduler;
        return this;
    }

    /**
     * Set a callback that will be passed in the {@link ConnectionFactory} of this {@link ReceiverOptions} instance to create a {@link Connection}.
     * <p>
     * Note the created connection will be used by a {@link Receiver} instance, which will cache it for re-use in its operations
     * and close it when {@link Receiver#close()} is called.
     *
     * @param function callback to create a {@link Connection}
     * @return this current {@link ReceiverOptions} instance
     */
    public ReceiverOptions connectionSupplier(Utils.ExceptionFunction<ConnectionFactory, ? extends Connection> function) {
        return this.connectionSupplier(this.connectionFactory, function);
    }

    /**
     * Set a callback that will be passed in the given {@link ConnectionFactory} to create a {@link Connection}.
     * <p>
     * Note the created connection will be used by a {@link Receiver} instance, which will cache it for re-use in its operations
     * and close it when {@link Receiver#close()} is called.
     *
     * @param connectionFactory the {@link ConnectionFactory} passed-in to the creation function
     * @param function          callback to create a {@link Connection}
     * @return this current {@link ReceiverOptions}
     */
    public ReceiverOptions connectionSupplier(ConnectionFactory connectionFactory, Utils.ExceptionFunction<ConnectionFactory, ? extends Connection> function) {
        this.connectionSupplier = ignored -> function.apply(connectionFactory);
        return this;
    }

    /**
     * Send a {@link Mono} that the created {@link Receiver} will use for its operations.
     * <p>
     * A {@link Receiver} created from this {@link ReceiverOptions} instance will not cache for re-use, nor close
     * on {@link Receiver#close()} the underlying connection. It is recommended that the passed-in {@link Mono} handles
     * caching of some sort to avoid a new connection to be created every time the {@link Receiver} does an operation.
     * It is the developer's responsibility to close the underlying {@link Connection} once the {@link Receiver} is closed
     * and no longer needs it.
     *
     * @param connectionMono
     * @return this current {@link ReceiverOptions}
     */
    public ReceiverOptions connectionMono(Mono<? extends Connection> connectionMono) {
        this.connectionMono = connectionMono;
        return this;
    }

    public Mono<? extends Connection> getConnectionMono() {
        return connectionMono;
    }

    public Utils.ExceptionFunction<ConnectionFactory, ? extends Connection> getConnectionSupplier() {
        return connectionSupplier;
    }
}
