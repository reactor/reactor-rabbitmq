/*
 * Copyright (c) 2018 Pivotal Software Inc, All Rights Reserved.
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

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Set of utilities.
 */
public abstract class Utils {

    public static Mono<? extends Connection> singleConnectionMono(ConnectionFactory connectionFactory) {
        return singleConnectionMono(connectionFactory, cf -> cf.newConnection());
    }

    public static Mono<? extends Connection> singleConnectionMono(ConnectionFactory cf, ExceptionFunction<ConnectionFactory, ? extends Connection> function) {
        return Mono.fromCallable(() -> new IdempotentClosedConnection(function.apply(cf))).cache();
    }

    public static Mono<? extends Connection> singleConnectionMono(Callable<? extends Connection> supplier) {
        return Mono.fromCallable(() -> new IdempotentClosedConnection(supplier.call())).cache();
    }

    public static Utils.ExceptionFunction<ConnectionFactory, ? extends Connection> singleConnectionSupplier(
            ConnectionFactory cf, Utils.ExceptionFunction<ConnectionFactory, ? extends Connection> supplier) {
        return new SingleConnectionSupplier(() -> supplier.apply(cf));
    }

    public static Utils.ExceptionFunction<ConnectionFactory, ? extends Connection> singleConnectionSupplier(ConnectionFactory cf) {
        return new SingleConnectionSupplier(() -> cf.newConnection());
    }

    public static Utils.ExceptionFunction<ConnectionFactory, ? extends Connection> singleConnectionSupplier(Callable<? extends Connection> supplier) {
        return new SingleConnectionSupplier(supplier);
    }

    @FunctionalInterface
    public interface ExceptionFunction<T, R> {

        R apply(T t) throws Exception;
    }

    public static class SingleConnectionSupplier implements Utils.ExceptionFunction<ConnectionFactory, Connection> {

        private final Callable<? extends Connection> creationAction;
        private final Duration waitTimeout;

        private final CountDownLatch latch = new CountDownLatch(1);
        private AtomicBoolean created = new AtomicBoolean(false);
        private AtomicReference<Connection> connection = new AtomicReference<>();

        public SingleConnectionSupplier(Callable<? extends Connection> creationAction) {
            this(creationAction, Duration.ofMinutes(5));
        }

        public SingleConnectionSupplier(Callable<? extends Connection> creationAction, Duration waitTimeout) {
            this.creationAction = creationAction;
            this.waitTimeout = waitTimeout;
        }

        @Override
        public Connection apply(ConnectionFactory connectionFactory) throws Exception {
            if (created.compareAndSet(false, true)) {
                connection.set(new IdempotentClosedConnection(creationAction.call()));
                latch.countDown();
            } else {
                boolean reachedZero = latch.await(waitTimeout.toMillis(), TimeUnit.MILLISECONDS);
                if (!reachedZero) {
                    if (connection.get() != null) { // if lucky
                        return connection.get();
                    }
                    throw new RabbitFluxException("Reached timeout when waiting for connection to be created: " + waitTimeout);
                }
            }
            return connection.get();
        }
    }
}
