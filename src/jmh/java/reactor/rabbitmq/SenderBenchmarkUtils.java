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
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import reactor.core.publisher.Flux;

import java.util.UUID;

public class SenderBenchmarkUtils {

    public static Flux<OutboundMessage> outboundMessageFlux(String queue, int nbMessages) {
        return Flux.range(0, nbMessages).map(i -> new OutboundMessage("", queue, "".getBytes()));
    }

    public static Connection newConnection() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        return connectionFactory.newConnection();
    }

    public static String declareQueue(Connection connection) throws Exception {
        String queueName = UUID.randomUUID().toString();
        Channel channel = connection.createChannel();
        String queue = channel.queueDeclare(queueName, false, false, false, null).getQueue();
        channel.close();
        return queue;
    }

    public static void deleteQueue(Connection connection, String queue) throws Exception {
        Channel channel = connection.createChannel();
        channel.queueDelete(queue);
        channel.close();
    }

}
