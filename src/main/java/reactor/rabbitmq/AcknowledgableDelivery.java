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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Delivery;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * A RabbitMQ {@link Delivery} that can be manually acknowledged or rejected.
 */
public class AcknowledgableDelivery extends Delivery {

    private final Channel channel;
    private final BiConsumer<Receiver.AcknowledgmentContext, Exception> exceptionHandler;

    private final AtomicBoolean notAckedOrNacked = new AtomicBoolean(true);

    /**
     * Made public only for testing purposes.
     * Only the library is supposed to create instances.
     *
     * @param delivery
     * @param channel
     * @param exceptionHandler
     */
    public AcknowledgableDelivery(Delivery delivery, Channel channel, BiConsumer<Receiver.AcknowledgmentContext, Exception> exceptionHandler) {
        super(delivery.getEnvelope(), delivery.getProperties(), delivery.getBody());
        this.channel = channel;
        this.exceptionHandler = exceptionHandler;
    }

    /**
     * Acknowledges this message if it has not been previously acked or nacked.
     * Subsequent calls to the method for previously acknowledged message will not produce errors and will simply
     * return instantly.
     * In case of connection failure, {@link AcknowledgableDelivery#exceptionHandler} is executed.
     * @param multiple Defines whether all messages up to and including the supplied delivery tag should be
     * acknowledged or not.
     */
    public void ack(boolean multiple) {
        if (notAckedOrNacked.getAndSet(false)) {
            try {
                basicAck(multiple);
            } catch (Exception e) {
                retry(e, (delivery) -> delivery.basicAck(multiple));
            }
        }
    }

    /**
     * Acknowledges this message if it has not been previously acked or nacked.
     * Subsequent calls to the method for previously acknowledged message will not produce errors and will simply
     * return instantly.
     * In case of connection failure, {@link AcknowledgableDelivery#exceptionHandler} is executed.
     */
    public void ack() {
        ack(false);
    }

    /**
     * Rejects this message if it has not been previously acked or nacked.
     * Subsequent calls to the method for previously acknowledged or rejected message will not produce errors and
     * will simply return instantly.
     * In case of connection failure, {@link AcknowledgableDelivery#exceptionHandler} is executed.
     * @param multiple Defines whether all messages up to and including the supplied delivery tag should be
     * rejected or not.
     * @param requeue Defines if the message should be added to the queue again instead of being discarded.
     */
    public void nack(boolean multiple, boolean requeue) {
        if (notAckedOrNacked.getAndSet(false)) {
            try {
                basicNack(multiple, requeue);
            } catch (Exception e) {
                retry(e, (delivery) -> delivery.basicNack(multiple, requeue));
            }
        }
    }

    /**
     * Rejects this message if it has not been previously acked or nacked.
     * Subsequent calls to the method for previously acknowledged or rejected message will not produce errors and
     * will simply return instantly.
     * In case of connection failure, {@link AcknowledgableDelivery#exceptionHandler} is executed.
     */
    public void nack(boolean requeue) {
        nack(false, requeue);
    }

    private void basicAck(boolean multiple) {
        try {
            channel.basicAck(getEnvelope().getDeliveryTag(), multiple);
        } catch (RuntimeException e) {
            throw e;
        } catch (IOException e) {
            throw new RabbitFluxException(e);
        }
    }

    private void basicNack(boolean multiple, boolean requeue) {
        try {
            channel.basicNack(getEnvelope().getDeliveryTag(), multiple, requeue);
        } catch (RuntimeException e) {
            throw e;
        } catch (IOException e) {
            throw new RabbitFluxException(e);
        }
    }

    private void retry(Exception e, Consumer<AcknowledgableDelivery> consumer) {
        try {
            exceptionHandler.accept(new Receiver.AcknowledgmentContext(this, consumer), e);
        } catch (Exception e2) {
            notAckedOrNacked.set(true);
            throw e2;
        }
    }
}
