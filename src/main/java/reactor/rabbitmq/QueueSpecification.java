/*
 * Copyright (c) 2017 Pivotal Software Inc, All Rights Reserved.
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

import java.util.Map;

/**
 * Fluent API to specify the creation of a queue.
 */
public class QueueSpecification {

    private String name;
    private boolean durable = false;
    private boolean exclusive = false;
    private boolean autoDelete = false;
    private Map<String, Object> arguments;

    public static QueueSpecification queue() {
        return new QueueSpecification();
    }

    public static QueueSpecification queue(String name) {
        return new QueueSpecification().name(name);
    }

    public QueueSpecification name(String queue) {
        this.name = queue;
        return this;
    }

    public QueueSpecification durable(boolean durable) {
        this.durable = durable;
        return this;
    }

    public QueueSpecification exclusive(boolean exclusive) {
        this.exclusive = exclusive;
        return this;
    }

    public QueueSpecification autoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
        return this;
    }

    public QueueSpecification arguments(Map<String, Object> arguments) {
        this.arguments = arguments;
        return this;
    }

    public String getName() {
        return name;
    }

    public boolean isDurable() {
        return durable;
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public Map<String, Object> getArguments() {
        return arguments;
    }
}
