/*
 * Copyright (c) 2019-2021 VMware Inc. or its affiliates, All Rights Reserved.
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
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.util.function.BiConsumer;

/**
 * Contract to obtain a {@link Channel} and close when sending messages.
 * <p>
 * Implementations would typically make it possible to re-use
 * {@link Channel}s across different calls to <code>Sender#send*</code> methods.
 *
 * @see SendOptions
 * @see Sender#send(Publisher)
 * @see Sender#send(Publisher, SendOptions)
 * @see Sender#sendWithPublishConfirms(Publisher)
 * @see Sender#send(Publisher, SendOptions)
 * @since 1.1.0
 */
public interface ChannelPool extends AutoCloseable {

    /**
     * The {@link Channel} to use for sending a flux of messages.
     *
     * @return the channel mono to use
     */
    Mono<? extends Channel> getChannelMono();

    /**
     * The closing logic when the {@link Channel} is disposed.
     *
     * @return the closing logic to use
     */
    BiConsumer<SignalType, Channel> getChannelCloseHandler();

    /**
     * Close the pool when it is no longer necessary.
     */
    void close();

}
