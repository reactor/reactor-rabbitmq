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

import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;

/**
 * Options to when creating a {@link ChannelPool}.
 *
 * @see ChannelPoolFactory
 * @see LazyChannelPool
 * @since 1.1.0
 */
public class ChannelPoolOptions {

    private Integer maxCacheSize;

    private Scheduler subscriptionScheduler;

    /**
     * Set the maximum size of the pool.
     * <p>
     * Default is 5 channels.
     *
     * @param maxCacheSize
     * @return this {@link ChannelPoolOptions} instance
     */
    public ChannelPoolOptions maxCacheSize(int maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
        return this;
    }

    @Nullable
    public Integer getMaxCacheSize() {
        return maxCacheSize;
    }

    /**
     * Set the scheduler to use when opening {@link com.rabbitmq.client.Channel}s.
     *
     * @param subscriptionScheduler
     * @return this {@link ChannelPoolOptions} instance
     */
    public ChannelPoolOptions subscriptionScheduler(@Nullable Scheduler subscriptionScheduler) {
        this.subscriptionScheduler = subscriptionScheduler;
        return this;
    }

    @Nullable
    public Scheduler getSubscriptionScheduler() {
        return subscriptionScheduler;
    }
}
