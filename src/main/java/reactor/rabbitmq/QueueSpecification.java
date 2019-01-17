/*
 * Copyright (c) 2017-2019 Pivotal Software Inc, All Rights Reserved.
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
 * <p>
 * If a queue specification has a null name, the queue to be created
 * will have a server-generated name and will be non-durable, exclusive, and
 * auto-delete. To have more control over the properties of a queue with
 * a server-generated name, specify a non-null, empty string name, <code>""</code>.
 */
public class QueueSpecification {

    protected String name;
    protected boolean durable = false;
    protected boolean exclusive = false;
    protected boolean autoDelete = false;
    protected Map<String, Object> arguments;

    public static QueueSpecification queue() {
        return new NullNameQueueSpecification();
    }

    public static QueueSpecification queue(String name) {
        return new QueueSpecification().name(name);
    }

    public QueueSpecification name(String queue) {
        if (queue == null) {
            return new NullNameQueueSpecification().arguments(this.arguments);
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

    /**
     * Internal class to handle queues with a null name.
     * Those queues always have a server-generated name are non-durable,
     * exclusive, and auto-delete.
     */
    private static class NullNameQueueSpecification extends QueueSpecification {

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
                throw new IllegalArgumentException("Once a queue has a null name, durable is always false");
            }
            return this;
        }

        @Override
        public QueueSpecification exclusive(boolean exclusive) {
            if (this.exclusive != exclusive) {
                throw new IllegalArgumentException("Once a queue has a null name, exclusive is always true");
            }
            return this;
        }

        @Override
        public QueueSpecification autoDelete(boolean autoDelete) {
            if (this.autoDelete != autoDelete) {
                throw new IllegalArgumentException("Once a queue has a null name, autoDelete is always true");
            }
            return this;
        }
    }
}
