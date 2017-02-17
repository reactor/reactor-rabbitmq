/*
 * Copyright (c) 2017 Pivotal Software Inc, All Rights Reserved.
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

import com.rabbitmq.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 *
 */
public class ReactorRabbitMqTests {

    Connection connection;
    String queue;

    @Before
    public void init() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        String queueName = UUID.randomUUID().toString();
        queue = channel.queueDeclare(queueName, false, false, false, null).getQueue();
        channel.close();
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            Channel channel = connection.createChannel();
            channel.queueDelete(queue);
            channel.close();
            connection.close();
        }
    }

    @Test
    public void receiverConsumeNoAck() throws Exception {
        Channel channel = connection.createChannel();
        int nbMessages = 10;

        Receiver receiver = ReactorRabbitMq.createReceiver();

        for (int $ : IntStream.range(0, 10).toArray()) {
            Flux<Delivery> flux = receiver.consumeNoAck(queue);
            for (int $$ : IntStream.range(0, nbMessages).toArray()) {
                channel.basicPublish("", queue, null, "Hello".getBytes());
            }

            CountDownLatch latch = new CountDownLatch(nbMessages * 2);
            AtomicInteger counter = new AtomicInteger();
            Disposable subscription = flux.subscribe(msg -> {
                counter.incrementAndGet();
                latch.countDown();
            });

            for (int $$ : IntStream.range(0, nbMessages).toArray()) {
                channel.basicPublish("", queue, null, "Hello".getBytes());
            }

            assertTrue(latch.await(1, TimeUnit.SECONDS));
            subscription.dispose();
            assertEquals(nbMessages * 2, counter.get());
        }
        receiver.close();
        assertNull(connection.createChannel().basicGet(queue, true));
    }

    @Test
    public void receiverConsumeAutoAck() throws Exception {
        Channel channel = connection.createChannel();
        int nbMessages = 10;

        Receiver receiver = ReactorRabbitMq.createReceiver();

        for (int $ : IntStream.range(0, 10).toArray()) {
            Flux<Delivery> flux = receiver.consumeAutoAck(queue);

            for (int $$ : IntStream.range(0, nbMessages).toArray()) {
                channel.basicPublish("", queue, null, "Hello".getBytes());
            }

            CountDownLatch latch = new CountDownLatch(nbMessages * 2);
            AtomicInteger counter = new AtomicInteger();
            Disposable subscription = flux.subscribe(msg -> {
                counter.incrementAndGet();
                latch.countDown();
            });

            for (int $$ : IntStream.range(0, nbMessages).toArray()) {
                channel.basicPublish("", queue, null, "Hello".getBytes());
            }

            assertTrue(latch.await(1, TimeUnit.SECONDS));
            subscription.dispose();
            assertEquals(nbMessages * 2, counter.get());
        }

        receiver.close();
        assertNull(connection.createChannel().basicGet(queue, true));
    }

    @Test
    public void receiverConsumeManuelAck() throws Exception {
        Channel channel = connection.createChannel();
        int nbMessages = 10;

        Receiver receiver = ReactorRabbitMq.createReceiver();

        for (int $ : IntStream.range(0, 10).toArray()) {
            Flux<AcknowledgableDelivery> flux = receiver.consumeManuelAck(queue);

            for (int $$ : IntStream.range(0, nbMessages).toArray()) {
                channel.basicPublish("", queue, null, "Hello".getBytes());
            }

            CountDownLatch latch = new CountDownLatch(nbMessages * 2);
            AtomicInteger counter = new AtomicInteger();
            Disposable subscription = flux.bufferTimeout(5, Duration.ofSeconds(1)).subscribe(messages -> {
                counter.addAndGet(messages.size());
                messages.forEach(msg -> {
                    msg.ack();
                    latch.countDown();
                });
            });

            for (int $$ : IntStream.range(0, nbMessages).toArray()) {
                channel.basicPublish("", queue, null, "Hello".getBytes());
            }

            assertTrue(latch.await(1, TimeUnit.SECONDS));
            subscription.dispose();
            assertEquals(nbMessages * 2, counter.get());
        }

        receiver.close();
        assertNull(connection.createChannel().basicGet(queue, true));
    }

    @Test
    public void sender() throws Exception {
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

        Flux<OutboundMessage> msgFlux = Flux.range(0, nbMessages).map(i -> new OutboundMessage("", queue, "".getBytes()));

        Sender sender = ReactorRabbitMq.createSender();
        sender.send(msgFlux).subscribe();
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals(nbMessages, counter.get());
        sender.close();
    }

    @Test
    public void createResources() throws Exception {
        final Channel channel = connection.createChannel();

        final String queueName = UUID.randomUUID().toString();
        final String exchangeName = UUID.randomUUID().toString();

        try {
            Sender sender = ReactorRabbitMq.createSender();

            Mono<AMQP.Queue.BindOk> resources = sender.createExchange(ExchangeSpecification.exchange().exchange(exchangeName))
                .then(sender.createQueue(QueueSpecification.queue().queue(queueName)))
                .then(sender.bind(BindingSpecification.binding().queue(queueName).exchange(exchangeName).routingKey("a.b")));

            resources.block(java.time.Duration.ofSeconds(1));

            channel.exchangeDeclarePassive(exchangeName);
            channel.queueDeclarePassive(queueName);
        } finally {
            channel.exchangeDelete(exchangeName);
            channel.queueDelete(queueName);
            channel.close();
        }
    }
}
