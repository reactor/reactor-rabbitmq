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
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoverableConnection;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.impl.NetworkConnection;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static reactor.rabbitmq.ReactorRabbitMq.createSender;

/**
 *
 */
public class ConnectionRecoveryTests {

    private static final long RECOVERY_INTERVAL = 2000;

    Connection connection;
    String queue;

    Receiver receiver;
    Sender sender;

    Mono<Connection> connectionMono;

    public static Stream<BiFunction<Receiver, String, Flux<? extends Delivery>>> consumeNoAckArguments() {
        return Stream.of(
            (receiver, queue) -> receiver.consumeNoAck(queue, new ConsumeOptions().overflowStrategy(
                FluxSink.OverflowStrategy.BUFFER
            )),
            (receiver, queue) -> receiver.consumeAutoAck(queue),
            (receiver, queue) -> receiver.consumeManualAck(queue)
        );
    }

    private static void wait(CountDownLatch latch) throws InterruptedException {
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @BeforeEach
    public void init() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        connectionFactory.setNetworkRecoveryInterval(RECOVERY_INTERVAL);
        connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        String queueName = UUID.randomUUID().toString();
        queue = channel.queueDeclare(queueName, false, false, false, null).getQueue();
        channel.close();
        receiver = null;
        sender = null;
        connectionMono = Mono.just(connectionFactory.newConnection()).cache();
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
        if (receiver != null) {
            receiver.close();
        }
    }

    @ParameterizedTest
    @MethodSource("consumeNoAckArguments")
    public void consumeConsumerShouldRecoverAutomatically(BiFunction<Receiver, String, Flux<? extends Delivery>> deliveryFactory) throws Exception {
        Channel channel = connection.createChannel();
        int nbMessages = 10;

        receiver = ReactorRabbitMq.createReceiver(new ReceiverOptions().connectionMono(connectionMono));

        for (int $ : IntStream.range(0, 1).toArray()) {
            Flux<? extends Delivery> flux = deliveryFactory.apply(receiver, queue);
            for (int $$ : IntStream.range(0, nbMessages).toArray()) {
                channel.basicPublish("", queue, null, "Hello".getBytes());
            }

            CountDownLatch latch = new CountDownLatch(nbMessages * 2);
            AtomicInteger counter = new AtomicInteger();
            Disposable subscription = flux.subscribe(msg -> {
                counter.incrementAndGet();
                latch.countDown();
                if (msg instanceof AcknowledgableDelivery) {
                    ((AcknowledgableDelivery) msg).ack();
                }
            });

            closeAndWaitForRecovery((RecoverableConnection) connectionMono.block());

            for (int $$ : IntStream.range(0, nbMessages).toArray()) {
                channel.basicPublish("", queue, null, "Hello".getBytes());
            }

            assertTrue(latch.await(1, TimeUnit.SECONDS));
            subscription.dispose();
            assertEquals(nbMessages * 2, counter.get());
        }
        assertNull(connection.createChannel().basicGet(queue, true));
    }

    @Test
    public void sendRetryOnFailureAllFluxMessagesShouldBeSentAndConsumed() throws Exception {
        int nbMessages = 10;
        CountDownLatch latch = new CountDownLatch(nbMessages);
        AtomicInteger counter = new AtomicInteger();
        Channel channel = connection.createChannel();
        channel.basicConsume(queue, true, new DefaultConsumer(channel) {

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                counter.incrementAndGet();
                latch.countDown();
            }
        });

        Flux<OutboundMessage> msgFlux = Flux.range(0, nbMessages)
            .map(i -> new OutboundMessage("", queue, "".getBytes()))
            .delayElements(Duration.ofMillis(200));

        sender = createSender(new SenderOptions().connectionMono(connectionMono));
        sender.send(msgFlux, new SendOptions().exceptionHandler(
            new ExceptionHandlers.RetrySendingExceptionHandler(5_000, 100, Collections.singletonMap(
                AlreadyClosedException.class, true
            ))))
            .subscribe();

        closeAndWaitForRecovery((RecoverableConnection) connectionMono.block());

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals(nbMessages, counter.get());
    }

    @Test
    public void sendWithPublishConfirmsAllMessagesShouldBeSentConfirmedAndConsumed() throws Exception {
        int nbMessages = 10;
        CountDownLatch consumedLatch = new CountDownLatch(nbMessages);
        CountDownLatch confirmedLatch = new CountDownLatch(nbMessages);
        AtomicInteger counter = new AtomicInteger();
        Channel channel = connection.createChannel();
        channel.basicConsume(queue, true, (consumerTag, delivery) -> {
            counter.incrementAndGet();
            consumedLatch.countDown();
        }, consumerTag -> {
        });

        Flux<OutboundMessage> msgFlux = Flux.range(0, nbMessages)
            .map(i -> new OutboundMessage("", queue, "".getBytes()))
            .delayElements(Duration.ofMillis(300));

        sender = createSender(new SenderOptions().connectionMono(connectionMono));
        sender.sendWithPublishConfirms(msgFlux, new SendOptions().exceptionHandler(
            new ExceptionHandlers.RetrySendingExceptionHandler(5_000, 100, Collections.singletonMap(
                AlreadyClosedException.class, true
            ))))
            .subscribe(outboundMessageResult -> {
                if (outboundMessageResult.isAck() && outboundMessageResult.getOutboundMessage() != null) {
                    confirmedLatch.countDown();
                }
            });

        closeAndWaitForRecovery((RecoverableConnection) connectionMono.block());

        assertTrue(consumedLatch.await(10, TimeUnit.SECONDS));
        assertTrue(confirmedLatch.await(10, TimeUnit.SECONDS));
        assertEquals(nbMessages, counter.get());
    }

    private void closeAndWaitForRecovery(RecoverableConnection connection) throws IOException, InterruptedException {
        CountDownLatch latch = prepareForRecovery(connection);
        Host.closeConnection((NetworkConnection) connection);
        wait(latch);
    }

    private CountDownLatch prepareForRecovery(Connection conn) {
        final CountDownLatch latch = new CountDownLatch(1);
        ((AutorecoveringConnection) conn).addRecoveryListener(new RecoveryListener() {

            public void handleRecovery(Recoverable recoverable) {
                latch.countDown();
            }

            public void handleRecoveryStarted(Recoverable recoverable) {
                // No-op
            }
        });
        return latch;
    }
}
