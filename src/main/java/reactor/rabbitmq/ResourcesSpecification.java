/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import reactor.util.annotation.Nullable;

/**
 * API that combines {@link ExchangeSpecification}, {@link QueueSpecification}, and {@link BindingSpecification}.
 */
public class ResourcesSpecification {

    public static ExchangeSpecification exchange(String name) {
        return ExchangeSpecification.exchange(name);
    }

    public static QueueSpecification queue(@Nullable String name) {
        return QueueSpecification.queue(name);
    }

    public static BindingSpecification binding(String exchange, String routingKey, String queue) {
        return BindingSpecification.binding(exchange, routingKey, queue);
    }

}
