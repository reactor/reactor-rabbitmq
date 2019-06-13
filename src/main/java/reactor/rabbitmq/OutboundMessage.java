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

import com.rabbitmq.client.AMQP.BasicProperties;

import java.util.Arrays;

/**
 * Outbound message meant to be sent by a {@link Sender}.
 */
public class OutboundMessage {

    private final String exchange;

    private final String routingKey;

    private final BasicProperties properties;

    private final byte [] body;

    // TODO add a correlation property to use for OutboundMessageResult
    // (instead of using the whole message)

    /**
     * Constructs a new message which is described by the body, the target exchange and the routing key which
     * can be used for smart routing after the message is published to the exchange. The message will also be
     * accompanied by the provided properties which define its behaviour in the broker.
     * @param exchange The name of the target exchange.
     * @param routingKey The routing key to be used if the message has to be routed in a specific way towards a queue.
     * @param properties AMQP compatible properties that will be used during the publishing of the message.
     * @param body The main body of the message.
     */
    public OutboundMessage(String exchange, String routingKey, BasicProperties properties, byte[] body) {
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.properties = properties;
        this.body = body;
    }

    /**
     * Constructs a new message which is described by the body, the target exchange and the routing key which
     * can be used for smart routing after the message is published to the exchange.
     * @param exchange The name of the target exchange.
     * @param routingKey The routing key to be used if the message has to be routed in a specific way towards a queue.
     * @param body The main body of the message.
     */
    public OutboundMessage(String exchange, String routingKey, byte[] body) {
        this(exchange, routingKey, null, body);
    }

    /**
     * Defines the exchange to which the message will be published.
     * @return The exchange name.
     */
    public String getExchange() {
        return exchange;
    }

    /**
     * Defines the routing key to be used if the message has to be routed in a specific way towards a queue
     * @return The routing key
     */
    public String getRoutingKey() {
        return routingKey;
    }

    /**
     * Defines any additional properties that will be used during the publishing of the message.
     * @return All the properties that have been set. Null if no property is set.
     */
    public BasicProperties getProperties() {
        return properties;
    }

    /**
     * Defines the main body of the message in byte array form.
     * @return The body of the message
     */
    public byte[] getBody() {
        return body;
    }

    @Override
    public String toString() {
        return "OutboundMessage{" +
            "exchange='" + exchange + '\'' +
            ", routingKey='" + routingKey + '\'' +
            ", properties=" + properties +
            ", body=" + Arrays.toString(body) +
            '}';
    }
}
