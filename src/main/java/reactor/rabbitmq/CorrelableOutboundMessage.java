/*
 * Copyright (c) 2019 Pivotal Software Inc, All Rights Reserved.
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

import com.rabbitmq.client.AMQP;
import org.reactivestreams.Publisher;

import java.util.Arrays;

/**
 * An outbound message meant to be sent by a {@link Sender} and may contain (unserialized)
 * correlated metadata.
 *
 * @since 1.4.0
 * @see Sender#sendWithTypedPublishConfirms(Publisher)
 * @see Sender#sendWithTypedPublishConfirms(Publisher, SendOptions)
 */
public class CorrelableOutboundMessage<T> extends OutboundMessage {

    private final T correlationMetadata;

    /**
     * Constructs a new message which is described by the body, the target exchange and the routing key which
     * can be used for smart routing after the message is published to the exchange.
     *
     * @param exchange            The name of the target exchange.
     * @param routingKey          The routing key to be used if the message has to be routed in a specific way towards a queue.
     * @param body                The main body of the message.
     * @param correlationMetadata The (unserialized) metadata correlated with this Message.
     */
    public CorrelableOutboundMessage(String exchange, String routingKey, byte[] body, T correlationMetadata) {
        super(exchange, routingKey, body);
        this.correlationMetadata = correlationMetadata;
    }

    /**
     * Constructs a new message which is described by the body, the target exchange and the routing key which
     * can be used for smart routing after the message is published to the exchange. The message will also be
     * accompanied by the provided properties which define its behaviour in the broker.
     *
     * @param exchange            The name of the target exchange.
     * @param routingKey          The routing key to be used if the message has to be routed in a specific way towards a queue.
     * @param properties          AMQP compatible properties that will be used during the publishing of the message.
     * @param body                The main body of the message.
     * @param correlationMetadata The (unserialized) metadata correlated with this Message.
     */
    public CorrelableOutboundMessage(String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body, T correlationMetadata) {
        super(exchange, routingKey, properties, body);
        this.correlationMetadata = correlationMetadata;
    }

    /**
     * Defines the "correlation" metadata associated with a sent OutboundMessage
     *
     * @return The correlated metadata of the message
     */
    public T getCorrelationMetadata() {
        return correlationMetadata;
    }

    @Override
    public String toString() {
        return "CorrelableOutboundMessage{" +
                "exchange='" + getExchange() + '\'' +
                ", routingKey='" + getRoutingKey() + '\'' +
                ", properties=" + getProperties() + '\'' +
                ", body=" + Arrays.toString(getBody()) + '\'' +
                ", correlationMetadata=" + getCorrelationMetadata() +
                '}';
    }
}
