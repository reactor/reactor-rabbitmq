/*
 * Copyright (c) 2018-2019 Pivotal Software Inc, All Rights Reserved.
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
import com.rabbitmq.client.Connection;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;

import static reactor.rabbitmq.ChannelCloseHandlers.SENDER_CHANNEL_CLOSE_HANDLER_INSTANCE;

/**
 * This channel pool is lazy initialized. It might even not reach its maximum size {@link ChannelPoolOptions#getMaxSize()} in low-traffic environments.
 * It always tries to obtain channel from the pool. However, in case of high-traffic number of channels might exceeds channel pool maximum size.
 *
 * Channels are added to the pool after their use {@link ChannelPool#getChannelCloseHandler()} and obtained from the pool when channel is requested {@link ChannelPool#getChannelMono()}.
 *
 * If pool is empty, new channel is created.
 * If channel is no longer needed and the channel pool is full (high-traffic), then channel is being closed.
 * If channel is no longer needed and the channel pool has not reached its capacity, then channel is added to the pool.
 *
 * It uses {@link BlockingQueue} internally in a non-blocking way.
 *
 */
class LazyChannelPool implements ChannelPool {

    private final Mono<? extends Connection> connectionMono;
    private final BlockingQueue<Channel> channelsQueue;
    private final Scheduler subscriptionScheduler;

    LazyChannelPool(Mono<? extends Connection> connectionMono, ChannelPoolOptions channelPoolOptions) {
        this.channelsQueue = new LinkedBlockingQueue<>(channelPoolOptions.getMaxSize());
        this.connectionMono = connectionMono;
        this.subscriptionScheduler = channelPoolOptions.getSubscriptionScheduler();
    }

    public Mono<? extends Channel> getChannelMono() {
        return connectionMono.map(connection -> {
            Channel channel = channelsQueue.poll();
            if (channel == null) {
                channel = createChannel(connection);
            }
            return channel;
        }).subscribeOn(subscriptionScheduler);
    }

    @Override
    public BiConsumer<SignalType, Channel> getChannelCloseHandler() {
        return (signalType, channel) -> {
            if (!channel.isOpen()) {
                return;
            }
            // maybe also close channel if signalType == SignalType.ON_ERROR ?
            boolean offer = channelsQueue.offer(channel);
            if (!offer) {
                SENDER_CHANNEL_CLOSE_HANDLER_INSTANCE.accept(signalType, channel);
            }
        };
    }

    @Override
    public void close() {
        List<Channel> channels = new ArrayList<>();
        channelsQueue.drainTo(channels);
        channels.forEach(channel -> {
            SENDER_CHANNEL_CLOSE_HANDLER_INSTANCE.accept(SignalType.ON_COMPLETE, channel);
        });
    }

    private Channel createChannel(Connection connection) {
        try {
            return connection.createChannel();
        } catch (IOException e) {
            throw new RabbitFluxException("Error while creating channel", e);
        }
    }

}
