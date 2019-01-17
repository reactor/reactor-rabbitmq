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

    static class NullNameQueueSpecification extends QueueSpecification {

        NullNameQueueSpecification() {
            this.name = null;
            this.durable = false;
            this.exclusive = true;
            this.autoDelete = true;
        }

        @Override
        public QueueSpecification name(String name) {
            if (name == null) {
                return this;
            }
            return QueueSpecification.queue(name)
                .durable(durable)
                .exclusive(exclusive)
                .autoDelete(autoDelete);
        }

        @Override
        public QueueSpecification durable(boolean durable) {
            if (this.durable != durable) {
                throw new IllegalArgumentException("once a queue has null name, durable is always false");
            }
            return this;
        }

        @Override
        public QueueSpecification exclusive(boolean exclusive) {
            if (this.exclusive != exclusive) {
                throw new IllegalArgumentException("once a queue has null name, exclusive is always true");
            }
            return this;
        }

        @Override
        public QueueSpecification autoDelete(boolean autoDelete) {
            if (this.autoDelete != autoDelete) {
                throw new IllegalArgumentException("once a queue has null name, autoDelete is always true");
            }
            return this;
        }
    }

    String name;
    boolean durable = false;
    boolean exclusive = false;
    boolean autoDelete = false;
    Map<String, Object> arguments;

    public static QueueSpecification queue() {
        return new NullNameQueueSpecification();
    }

    public static QueueSpecification queue(String name) {
        return new QueueSpecification().name(name);
    }

    public QueueSpecification name(String queue) {
        if (queue == null) {
            return new NullNameQueueSpecification();
        }

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
