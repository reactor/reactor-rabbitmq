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

import reactor.util.annotation.Nullable;

import java.util.Map;

/**
 * Fluent API to specify the binding between AMQP resources.
 * Support exchange-to-queue and exchange-to-exchange binding definition.
 */
public class BindingSpecification {

    private String queue, exchange, exchangeTo, routingKey;
    private Map<String, Object> arguments;

    public static BindingSpecification binding() {
        return new BindingSpecification();
    }

    /**
     * Create an exchange-to-queue binding specification.
     *
     * @param exchange
     * @param routingKey
     * @param queue
     * @return this specification instance
     */
    public static BindingSpecification binding(String exchange, String routingKey, String queue) {
        return new BindingSpecification().exchange(exchange).routingKey(routingKey).queue(queue);
    }

    /**
     * Create an exchange-to-queue binding specification.
     *
     * @param exchange
     * @param routingKey
     * @param queue
     * @return this specification instance
     * @since 1.4.1
     */
    public static BindingSpecification queueBinding(String exchange, String routingKey, String queue) {
        return binding(exchange, routingKey, queue);
    }

    /**
     * Creates an exchange-to-exchange binding specification.
     *
     * @param exchangeFrom
     * @param routingKey
     * @param exchangeTo
     * @return this specification instance
     * @since 1.4.1
     */
    public static BindingSpecification exchangeBinding(String exchangeFrom, String routingKey, String exchangeTo) {
        return new BindingSpecification().exchangeFrom(exchangeFrom).routingKey(routingKey).exchangeTo(exchangeTo);
    }

    /**
     * The queue to bind to.
     * <p>
     * Use this method for exchange-to-queue binding or
     * {@link #exchangeTo(String)} for exchange-to-exchange, but not both.
     *
     * @param queue
     * @return this specification instance
     */
    public BindingSpecification queue(String queue) {
        this.queue = queue;
        return this;
    }

    /**
     * The exchange to bind from.
     * <p>
     * Alias of {@link #exchangeFrom(String)}. Usually used for
     * exchange-to-queue binding, but can be used for exchange-to-exchange
     * binding as well.
     *
     * @param exchange
     * @return this specification instance
     * @see #exchangeFrom(String)
     * @see #exchangeTo(String)
     */
    public BindingSpecification exchange(String exchange) {
        this.exchange = exchange;
        return this;
    }

    /**
     * The exchange to bind from.
     * <p>
     * Alias of {@link #exchange(String)}. Usually used to make explicit
     * the definition is for an exchange-to-exchange binding, but works for
     * exchange-to-queue binding as well.
     *
     * @param exchangeFrom
     * @return this specification instance
     * @see #exchange(String)
     * @see #exchangeTo(String)
     * @since 1.4.1
     */
    public BindingSpecification exchangeFrom(String exchangeFrom) {
        this.exchange = exchangeFrom;
        return this;
    }

    /**
     * The exchange to bind to.
     * <p>
     * Use this method for exchange-to-exchange binding or
     * {@link #queue(String)} for exchange-to-queue binding, but not both.
     *
     * @param exchangeTo
     * @return this specification instance
     * @since 1.4.1
     */
    public BindingSpecification exchangeTo(String exchangeTo) {
        this.exchangeTo = exchangeTo;
        return this;
    }

    /**
     * The routing key for the binding.
     *
     * @param routingKey
     * @return this specification instance
     */
    public BindingSpecification routingKey(String routingKey) {
        this.routingKey = routingKey;
        return this;
    }

    /**
     * Arguments of the binding. These are optional.
     *
     * @param arguments
     * @return this specification instance
     */
    public BindingSpecification arguments(@Nullable Map<String, Object> arguments) {
        this.arguments = arguments;
        return this;
    }

    public String getQueue() {
        return queue;
    }

    public String getExchange() {
        return exchange;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public String getExchangeTo() {
        return exchangeTo;
    }

    @Nullable
    public Map<String, Object> getArguments() {
        return arguments;
    }
}
