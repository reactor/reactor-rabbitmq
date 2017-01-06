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
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class FluxTests {

    Connection connection;

    @Before public void init() throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        connection = connectionFactory.newConnection();
    }

    @After public void tearDown() throws IOException {
        if(connection != null) {
            connection.close();
        }
    }

    @Test public void senderFlux() throws Exception {
        int nbMessages = 10;
        Flux<Integer> flux = Flux.range(1, nbMessages);
        Channel channel = connection.createChannel();
        AMQP.Queue.DeclareOk queue = channel.queueDeclare();

        CountDownLatch latch = new CountDownLatch(nbMessages);
        channel.basicConsume(queue.getQueue(), new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                latch.countDown();
            }
        });

        channel.confirmSelect();

        Flux<Tuple2<Long, Boolean>> publisherConfirms = Flux.create(emitter -> {
            channel.addConfirmListener(new ConfirmListener() {

                @Override
                public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                    emitter.next(Tuples.of(deliveryTag, multiple));
                }

                @Override
                public void handleNack(long deliveryTag, boolean multiple) throws IOException {

                }
            });
        });

        AtomicLong lastConfirm = new AtomicLong();
        publisherConfirms.subscribe(confirm -> lastConfirm.set(confirm.getT1()));

        flux.subscribe(value -> {
            try {

                channel.getNextPublishSeqNo();
                channel.basicPublish("", queue.getQueue(), null, String.valueOf(value).getBytes());

            } catch(Exception e) {
                e.printStackTrace();
            }
        });

        latch.await(1, TimeUnit.SECONDS);
        assertEquals(nbMessages, lastConfirm.get());
    }

    @Test public void senderFluxWithPublisherConfirms() throws Exception {
        int nbMessages = 10;
        Flux<Integer> flux = Flux.range(1, nbMessages);
        Channel channel = connection.createChannel();
        AMQP.Queue.DeclareOk queue = channel.queueDeclare();

        CountDownLatch latch = new CountDownLatch(nbMessages);
        channel.basicConsume(queue.getQueue(), new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                latch.countDown();
            }
        });

        flux.subscribe(value -> {
            try {
                channel.basicPublish("", queue.getQueue(), null, String.valueOf(value).getBytes());
            } catch(Exception e) {
                e.printStackTrace();
            }
        });

        latch.await(1, TimeUnit.SECONDS);
    }

    @Test public void consumerFlux() throws IOException, InterruptedException {
        final Channel channel = connection.createChannel();
        AMQP.Queue.DeclareOk queue = channel.queueDeclare();
        int nbMessages = 10;

        CountDownLatch cancelLatch = new CountDownLatch(1);

        Flux<Delivery> flux = Flux.create(emitter -> {
            final DefaultConsumer consumer = new DefaultConsumer(channel) {

                volatile int counter = 0;

                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    counter++;
                    if(counter <= nbMessages) {
                        emitter.next(new Delivery(envelope, properties, body));
                    } else {
                        emitter.complete();
                    }
                }

            };

            try {
                final String consumerTag = channel.basicConsume(queue.getQueue(), true, consumer);
                emitter.setCancellation(() -> {
                    try {
                        if(channel.isOpen() && channel.getConnection().isOpen()) {
                            channel.basicCancel(consumerTag);
                        }
                        cancelLatch.countDown();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, FluxSink.OverflowStrategy.BUFFER);

        for(int i = 0; i < nbMessages + 1; i++) {
            channel.basicPublish("", queue.getQueue(), null, "Hello".getBytes());
        }

        StepVerifier.create(flux).expectNextCount(nbMessages).expectComplete().verify();

        assertTrue(cancelLatch.await(1, TimeUnit.SECONDS));
    }


}
