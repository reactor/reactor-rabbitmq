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
import reactor.core.Disposable;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static reactor.rabbitmq.RabbitFlux.createReceiver;

/**
 *
 */
public class ReceiverTests {

    Receiver receiver;

    @BeforeEach
    public void init() {
        receiver = null;
    }

    @AfterEach
    public void tearDown() {
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
}
