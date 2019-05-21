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
 * Fluent API to specify creation of an exchange.
 */
public class ExchangeSpecification {

    private String name;
    private String type = "direct";
    private boolean durable = false, autoDelete = false, internal = false, passive = false;
    private Map<String, Object> arguments;

    public static ExchangeSpecification exchange() {
        return new ExchangeSpecification();
    }

    public static ExchangeSpecification exchange(String name) {
        return new ExchangeSpecification().name(name);
    }

    public ExchangeSpecification name(String name) {
        this.name = name;
        return this;
    }

    public ExchangeSpecification type(String type) {
        this.type = type;
        return this;
    }

    public ExchangeSpecification durable(boolean durable) {
        this.durable = durable;
        return this;
    }

    public ExchangeSpecification autoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
        return this;
    }

    public ExchangeSpecification internal(boolean internal) {
        this.internal = internal;
        return this;
    }

    public ExchangeSpecification passive(boolean passive) {
        this.passive = passive;
        return this;
    }

    public ExchangeSpecification arguments(Map<String, Object> arguments) {
        this.arguments = arguments;
        return this;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public boolean isDurable() {
        return durable;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public boolean isInternal() {
        return internal;
    }

    public boolean isPassive() {
        return passive;
    }

    public Map<String, Object> getArguments() {
        return arguments;
    }

}
