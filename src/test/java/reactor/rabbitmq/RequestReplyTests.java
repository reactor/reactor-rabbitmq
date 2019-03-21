/*
 * Copyright (c) 2018 Pivotal Software Inc, All Rights Reserved.
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

import com.rabbitmq.client.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class RequestReplyTests {

    static final List<String> QUEUES = Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    Connection serverConnection;
    Channel serverChannel;
    RpcServer rpcServer;
    Sender sender;

    public static Stream<Arguments> requestReplyParameters() {
        return Stream.of(
                Arguments.of(QUEUES.get(0), (Function<Sender, RpcClient>) sender -> sender.rpcClient("", QUEUES.get(0))),
                Arguments.of(QUEUES.get(1), (Function<Sender, RpcClient>) sender -> sender.rpcClient("", QUEUES.get(1), () -> UUID.randomUUID().toString()))
        );
    }

    @BeforeAll
    public static void initAll() throws Exception {
        try (Connection c = new ConnectionFactory().newConnection()) {
            Channel ch = c.createChannel();
            for (String queue : QUEUES) {
                ch.queueDeclare(queue, false, false, false, null);
            }
        }
    }

    @AfterAll
    public static void tearDownAll() throws Exception {
        try (Connection c = new ConnectionFactory().newConnection()) {
            Channel ch = c.createChannel();
            for (String queue : QUEUES) {
                ch.queueDelete(queue);
            }
        }
    }

    @BeforeEach
    public void init() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        serverConnection = connectionFactory.newConnection();
        serverChannel = serverConnection.createChannel();

    }

    @AfterEach
    public void tearDown() throws Exception {
        if (rpcServer != null) {
            rpcServer.terminateMainloop();
        }
        if (sender != null) {
            sender.close();
        }
    }

    @ParameterizedTest
    @MethodSource("requestReplyParameters")
    public void requestReply(String queue, Function<Sender, RpcClient> rpcClientCreator) throws Exception {
        rpcServer = new TestRpcServer(serverChannel, queue);
        new Thread(() -> {
            try {
                rpcServer.mainloop();
            } catch (Exception e) {
                // safe to ignore when loops ends/server is canceled
            }
        }).start();

        sender = RabbitFlux.createSender();

        int nbRequests = 10;
        CountDownLatch latch = new CountDownLatch(nbRequests);
        try (RpcClient rpcClient = rpcClientCreator.apply(sender)) {
            IntStream.range(0, nbRequests).forEach(i -> {
                String content = "hello " + i;
                Mono<Delivery> deliveryMono = rpcClient.rpc(Mono.just(new RpcClient.RpcRequest(content.getBytes())));
                String received = new String(deliveryMono.block().getBody());
                assertEquals("*** " + content + " ***", received);
                latch.countDown();
            });
            assertTrue(latch.await(5, TimeUnit.SECONDS), "All requests should have dealt with by now");
        }

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
