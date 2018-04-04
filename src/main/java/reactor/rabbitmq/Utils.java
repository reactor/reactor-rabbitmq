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

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import reactor.core.publisher.Mono;

import java.util.concurrent.Callable;

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

    @FunctionalInterface
    public interface ExceptionFunction<T, R> {

        R apply(T t) throws Exception;
    }
}
