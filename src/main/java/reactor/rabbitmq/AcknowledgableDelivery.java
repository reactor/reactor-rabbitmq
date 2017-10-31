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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Delivery;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A message that can be manually acknowledged.
 */
public class AcknowledgableDelivery extends Delivery {

    private final Channel channel;

    private final AtomicBoolean notAckedOrNacked = new AtomicBoolean(true);

    /**
     * Made public only for testing purposes.
     * Only the library is supposed to create instances.
     * @param delivery
     * @param channel
     */
    public AcknowledgableDelivery(Delivery delivery, Channel channel) {
        super(delivery.getEnvelope(), delivery.getProperties(), delivery.getBody());
        this.channel = channel;
    }

    public void ack(boolean multiple) {
        if(notAckedOrNacked.getAndSet(false)) {
            try {
                channel.basicAck(getEnvelope().getDeliveryTag(), multiple);
            } catch (IOException e) {
                throw new ReactorRabbitMqException(e);
            }
        }
    }

    public void ack() {
        ack(false);
    }

    public void nack(boolean multiple, boolean requeue) {
        if(notAckedOrNacked.getAndSet(false)) {
            try {
                channel.basicNack(getEnvelope().getDeliveryTag(), multiple, requeue);
            } catch (IOException e) {
                throw new ReactorRabbitMqException(e);
            }
        }
    }

    public void nack(boolean requeue) {
        nack(false, requeue);
    }
}
