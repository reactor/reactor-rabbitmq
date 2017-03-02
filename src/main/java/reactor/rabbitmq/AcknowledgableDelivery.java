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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

import java.io.IOException;

/**
 *
 */
public class AcknowledgableDelivery extends Delivery {

    private final Channel channel;

    public AcknowledgableDelivery(Envelope envelope, AMQP.BasicProperties properties, byte[] body, Channel channel) {
        super(envelope, properties, body);
        this.channel = channel;
    }

    public void ack() {
        // TODO make call idempotent
        try {
            channel.basicAck(getEnvelope().getDeliveryTag(), false);
        } catch (IOException e) {
            throw new ReactorRabbitMqException(e);
        }
    }

    public void nack(boolean requeue) {
        // TODO make call idempotent
        try {
            channel.basicNack(getEnvelope().getDeliveryTag(), false, requeue);
        } catch (IOException e) {
            throw new ReactorRabbitMqException(e);
        }
    }
}
