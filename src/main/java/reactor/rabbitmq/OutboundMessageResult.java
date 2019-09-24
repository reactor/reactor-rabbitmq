/*
 * Copyright (c) 2017-2019 Pivotal Software Inc, All Rights Reserved.
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

/**
 * Result of a sent message when using publisher confirm.
 */
public class OutboundMessageResult<T> {

    private final OutboundMessage<T> outboundMessage;

    private final boolean ack;

    private final boolean returned;

    /**
     * Constructs a result which is described by the initial message that has been published and the acknowledgment
     * status.
     *
     * @param outboundMessage The message that has been published
     * @param ack             Whether the message has been acknowledged by the broker or not
     */
    public OutboundMessageResult(OutboundMessage<T> outboundMessage, boolean ack) {
        this(outboundMessage, ack, false);
    }

    /**
     * Constructs a result which is described by the initial message that has been published, the acknowledgment
     * status, and the returned status.
     *
     * @param outboundMessage The message that has been published
     * @param ack             Whether the message has been acknowledged by the broker or not
     * @param returned        Whether the message was undeliverable and hence returned
     */
    public OutboundMessageResult(OutboundMessage<T> outboundMessage, boolean ack, boolean returned) {
        this.outboundMessage = outboundMessage;
        this.ack = ack;
        this.returned = returned;
    }

    /**
     * Defines the message that has been published.
     *
     * @return The message that has been published.
     */
    public OutboundMessage<T> getOutboundMessage() {
        return outboundMessage;
    }

    /**
     * Defines whether the message has been acknowledged by the broker or not. The message may still be confirmed if it
     * could not be routed to the correct queue. This can be validated with the {@link #isReturned() isReturned} method.
     *
     * @return True if the message has been acknowledged, false otherwise.
     */
    public boolean isAck() {
        return ack;
    }

    /**
     * Defines whether the message has been returned by the broker or not.
     *
     * @return True if the message was undeliverable and has returned, false otherwise.
     * @since 1.3.0
     */
    public boolean isReturned() {
        return returned;
    }

    @Override
    public String toString() {
        return "OutboundMessageResult{" +
                "outboundMessage=" + outboundMessage +
                ", ack=" + ack +
                ", returned=" + returned +
                '}';
    }
}
