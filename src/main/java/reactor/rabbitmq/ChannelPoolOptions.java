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

import reactor.core.scheduler.Scheduler;

public class ChannelPoolOptions {

    private Integer maxCacheSize;

    private Scheduler subscriptionScheduler;

    public ChannelPoolOptions maxCacheSize(int maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
        return this;
    }

    public Integer getMaxCacheSize() {
        return maxCacheSize;
    }

    public ChannelPoolOptions subscriptionScheduler(Scheduler subscriptionScheduler) {
        this.subscriptionScheduler = subscriptionScheduler;
        return this;
    }

    public Scheduler getSubscriptionScheduler() {
        return subscriptionScheduler;
    }
}
