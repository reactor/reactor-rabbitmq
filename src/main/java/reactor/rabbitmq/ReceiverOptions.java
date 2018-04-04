/*
 * Copyright (c) 2017-2018 Pivotal Software Inc, All Rights Reserved.
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
     * @param connectionSubscriptionScheduler
     * @return the current {@link ReceiverOptions} instance
     */
    public ReceiverOptions connectionSubscriptionScheduler(Scheduler connectionSubscriptionScheduler) {
        this.connectionSubscriptionScheduler = connectionSubscriptionScheduler;
        return this;
    }

    public ReceiverOptions connectionSupplier(Utils.ExceptionFunction<ConnectionFactory, ? extends Connection> function) {
        return this.connectionSupplier(this.connectionFactory, function);
    }

    public ReceiverOptions connectionSupplier(ConnectionFactory connectionFactory, Utils.ExceptionFunction<ConnectionFactory, ? extends Connection> function) {
        this.connectionMono = Mono.fromCallable(() -> function.apply(connectionFactory));
        return this;
    }

    public ReceiverOptions connectionMono(Mono<? extends Connection> connectionMono) {
        this.connectionMono = connectionMono;
        return this;
    }

    public Mono<? extends Connection> getConnectionMono() {
        return connectionMono;
    }
}
