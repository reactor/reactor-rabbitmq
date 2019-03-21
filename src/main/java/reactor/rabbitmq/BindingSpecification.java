/*
 * Copyright (c) 2017 Pivotal Software Inc, All Rights Reserved.
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

import java.util.Map;

/**
 * Fluent API to specify the binding between an exchange and a queue.
 */
public class BindingSpecification {

    private String queue, exchange, routingKey;
    private Map<String, Object> arguments;

    public static BindingSpecification binding() {
        return new BindingSpecification();
    }

    public static BindingSpecification binding(String exchange, String routingKey, String queue) {
        return new BindingSpecification().exchange(exchange).routingKey(routingKey).queue(queue);
    }

    public BindingSpecification queue(String queue) {
        this.queue = queue;
        return this;
    }

    public BindingSpecification exchange(String exchange) {
        this.exchange = exchange;
        return this;
    }

    public BindingSpecification routingKey(String routingKey) {
        this.routingKey = routingKey;
        return this;
    }

    public BindingSpecification arguments(Map<String, Object> arguments) {
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

    public Map<String, Object> getArguments() {
        return arguments;
    }
}
