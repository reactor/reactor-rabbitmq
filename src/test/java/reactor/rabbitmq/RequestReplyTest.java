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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.RpcServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class RequestReplyTest {

    Connection serverConnection;
    Channel serverChannel;
    String queue = "rpc.queue";
    RpcServer rpcServer;
    Sender sender;

    @BeforeEach
    public void init() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        serverConnection = connectionFactory.newConnection();
        serverChannel = serverConnection.createChannel();
        serverChannel.queueDeclare(queue, false, false, false, null);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (rpcServer != null) {
            rpcServer.terminateMainloop();
        }
        if (serverChannel != null) {
            serverChannel.queueDelete(queue);
        }
        if (sender != null) {
            sender.close();
        }
    }

    @Test
    public void requestReply() throws Exception {
        rpcServer = new TestRpcServer(serverChannel, queue);
        new Thread(() -> {
            try {
                rpcServer.mainloop();
            } catch (Exception e) {
                // safe to ignore when loops ends/server is canceled
            }
        }).start();

        sender = ReactorRabbitMq.createSender();

        int nbRequests = 10;
        CountDownLatch latch = new CountDownLatch(nbRequests);
        try (RpcClient rpcClient = sender.rpcClient("", queue)) {
            IntStream.range(0, nbRequests).forEach(i -> {
                new Thread(() -> {
                    String content = "hello " + i;
                    Mono<Delivery> deliveryMono = rpcClient.rpc(Mono.just(new RpcClient.RpcRequest(content.getBytes())));
                    assertEquals("*** " + content + " ***", new String(deliveryMono.block().getBody()));
                    latch.countDown();
                }).start();
            });
        }
        assertTrue(latch.await(5, TimeUnit.SECONDS), "All requests should have dealt with by now");
    }

    private static class TestRpcServer extends RpcServer {

        public TestRpcServer(Channel channel, String queueName) throws IOException {
            super(channel, queueName);
        }

        @Override
        public byte[] handleCall(Delivery request, AMQP.BasicProperties replyProperties) {
            String input = new String(request.getBody());
            return ("*** " + input + " ***").getBytes();
        }
    }
}
