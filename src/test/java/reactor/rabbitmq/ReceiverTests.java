/*
 * Copyright (c) 2019 Pivotal Software Inc, All Rights Reserved.
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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.Disposable;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;
import static reactor.rabbitmq.RabbitFlux.createReceiver;

/**
 *
 */
public class ReceiverTests {

    Connection connection;
    String queue;
    Receiver receiver;

    @BeforeEach
    public void init() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        String queueName = UUID.randomUUID().toString();
        queue = channel.queueDeclare(queueName, false, false, false, null).getQueue();
        channel.close();
        receiver = null;
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (connection != null) {
            Channel channel = connection.createChannel();
            channel.queueDelete(queue);
            channel.close();
            connection.close();
        }
        if (receiver != null) {
            receiver.close();
        }
    }

    @Test
    void connectionFromSupplierShouldBeCached() throws Exception {
        ConnectionFactory cf = new ConnectionFactory();
        cf.useNio();
        Connection c = cf.newConnection();
        Channel ch = c.createChannel();
        String q1 = ch.queueDeclare().getQueue();
        String q2 = ch.queueDeclare().getQueue();
        AtomicInteger callToConnectionSupplierCount = new AtomicInteger(0);
        ReceiverOptions options = new ReceiverOptions();
        options.connectionSupplier(cf, connectionFactory -> {
            callToConnectionSupplierCount.incrementAndGet();
            return c;
        });

        int messageCount = 10;
        CountDownLatch latch1 = new CountDownLatch(messageCount);
        CountDownLatch latch2 = new CountDownLatch(messageCount);
        receiver = createReceiver(options);

        Disposable registration1 = receiver.consumeAutoAck(q1).subscribe((msg) -> latch1.countDown());
        Disposable registration2 = receiver.consumeAutoAck(q2).subscribe((msg) -> latch2.countDown());

        for (int i = 0; i < messageCount; i++) {
            ch.basicPublish("", q1, null, "".getBytes());
            ch.basicPublish("", q2, null, "".getBytes());
        }

        assertTrue(latch1.await(10, TimeUnit.SECONDS));
        assertTrue(latch2.await(10, TimeUnit.SECONDS));
        registration1.dispose();
        registration2.dispose();
        assertEquals(1, callToConnectionSupplierCount.get());
    }

    @ValueSource(ints = {-1, 0, 10000})
    @ParameterizedTest
    public void connectionIsClosedWithDefaultTimeoutAndOverriddenValue(int timeoutInMs) throws Exception {
        Connection c = mock(Connection.class);
        Channel ch = mock(Channel.class);
        CountDownLatch channelCreatedLatch = new CountDownLatch(1);
        when(c.createChannel()).thenAnswer(invocation -> {
            channelCreatedLatch.countDown();
            return ch;
        });


        ReceiverOptions options = new ReceiverOptions()
                .connectionSupplier(cf -> c);

        if (timeoutInMs > 0) {
            // override
            options.connectionClosingTimeout(Duration.ofMillis(timeoutInMs));
        } else if (timeoutInMs == 0) {
            // default
            timeoutInMs = (int) options.getConnectionClosingTimeout().toMillis();
        } else {
            // no timeout
            options.connectionClosingTimeout(Duration.ZERO);
        }

        Receiver receiver = new Receiver(options);

        receiver.consumeNoAck("").subscribe();

        assertThat(channelCreatedLatch.await(5, TimeUnit.SECONDS)).isTrue();

        receiver.close();

        verify(c, times(1)).close(timeoutInMs);
    }

    @Test
    public void closeIsIdempotent() throws Exception {
        Receiver receiver = createReceiver();
        int nbMessages = 10;
        CountDownLatch latch = new CountDownLatch(nbMessages);
        Channel channel = connection.createChannel();
        for (int i = 0; i < nbMessages; i++) {
            channel.basicPublish("", queue, null, "".getBytes());
        }

        receiver.consumeNoAck(queue).subscribe(delivery -> latch.countDown());

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        receiver.close();
        receiver.close();
    }

    @Test
    public void consumeNoAckEmitsErrorWhenQueueDoesNotExist() {
        Receiver receiver = createReceiver();

        StepVerifier
                .create(receiver.consumeNoAck("not-existing-queue"))
                .expectError(RabbitFluxException.class)
                .verify(Duration.ofSeconds(3));
    }

    @Test
    public void consumeManualAckEmitsErrorWhenQueueDoesNotExist() {
        Receiver receiver = createReceiver();

        StepVerifier
                .create(receiver.consumeNoAck("not-existing-queue"))
                .expectError(RabbitFluxException.class)
                .verify(Duration.ofSeconds(3));
    }
}
