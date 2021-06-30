/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.rabbitmq;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.SignalType;

import java.util.function.BiConsumer;

/**
 * Helper class to close channels.
 *
 * @since 1.1.0
 */
public class ChannelCloseHandlers {

    public static final BiConsumer<SignalType, Channel> SENDER_CHANNEL_CLOSE_HANDLER_INSTANCE = new SenderChannelCloseHandler();

    /**
     * Default closing strategy in {@link Sender}.
     * <p>
     * Closes the channel and emits a warn-level log message in case of error.
     */
    public static class SenderChannelCloseHandler implements BiConsumer<SignalType, Channel> {

        private static final Logger LOGGER = LoggerFactory.getLogger(SenderChannelCloseHandler.class);

        @Override
        public void accept(SignalType signalType, Channel channel) {
            int channelNumber = channel.getChannelNumber();
            LOGGER.debug("closing channel {} by signal {}", channelNumber, signalType);
            try {
                if (channel.isOpen() && channel.getConnection().isOpen()) {
                    channel.close();
                }
            } catch (Exception e) {
                LOGGER.warn("Channel {} didn't close normally: {}", channelNumber, e.getMessage());
            }
        }
    }

}
