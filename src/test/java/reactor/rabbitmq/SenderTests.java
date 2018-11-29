/*
 * Copyright (c) 2018 Pivotal Software Inc, All Rights Reserved.
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
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownSignalException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.fail;
import static reactor.rabbitmq.RabbitFlux.createSender;

/**
 *
 */
public class SenderTests {

    Connection connection;
    String queue;
    Sender sender;

    @BeforeEach
    public void init() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        String queueName = UUID.randomUUID().toString();
        queue = channel.queueDeclare(queueName, false, false, false, null).getQueue();
        channel.close();
        sender = null;
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (connection != null) {
            Channel channel = connection.createChannel();
            channel.queueDelete(queue);
            channel.close();
            connection.close();
        }
        if (sender != null) {
            sender.close();
        }
    }

    @Test
    void cannotReuseChannelOnError() {
        sender = createSender();
        try {
            sender.declare(QueueSpecification.queue(queue).autoDelete(true)).block();
            fail("Trying to re-declare queue with different arguments, should have failed");
        } catch (ShutdownSignalException e) {
            // OK
        }
        sender.declare(QueueSpecification.queue()).block();
    }
}
