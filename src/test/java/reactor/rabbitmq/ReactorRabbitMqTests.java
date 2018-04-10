/*
 * Copyright (c) 2017-2018 Pivotal Software Inc, All Rights Reserved.
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
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static reactor.rabbitmq.ReactorRabbitMq.createReceiver;
import static reactor.rabbitmq.ReactorRabbitMq.createSender;
import static reactor.rabbitmq.ResourcesSpecification.binding;
import static reactor.rabbitmq.ResourcesSpecification.exchange;
import static reactor.rabbitmq.ResourcesSpecification.queue;

/**
 *
 */
public class ReactorRabbitMqTests {

    // TODO refactor test with StepVerifier

    Connection connection;
    String queue;

    Receiver receiver;
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
        receiver = null;
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
        if (receiver != null) {
            receiver.close();
        }
    }

    @Test public void senderCloseIsIdempotent() {
        sender = createSender();

        sender.send(Flux.just(new OutboundMessage("", "dummy", new byte[0])))
              .then().block();

        sender.close();
        sender.close();
    }

    @Test public void receiverCloseIsIdempotent() throws Exception {
        receiver = ReactorRabbitMq.createReceiver();

        sender = createSender();
        sender.send(Flux.just(new OutboundMessage("", queue, new byte[0]))).block();

        CountDownLatch latch = new CountDownLatch(1);
        Disposable subscribe = receiver.consumeAutoAck(queue).subscribe(delivery -> latch.countDown());

        assertTrue(latch.await(5, TimeUnit.SECONDS),"A message should have been received");
        subscribe.dispose();

        receiver.close();
        receiver.close();
    }

    @Test public void acknowledgableDeliveryAckNackIsIdempotent() throws Exception {
        Channel channel = mock(Channel.class);
        doNothing().doThrow(new IOException()).when(channel).basicAck(anyLong(), anyBoolean());
        doThrow(new IOException()).when(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
        AcknowledgableDelivery msg = new AcknowledgableDelivery(
            new Delivery(new Envelope(0, true, null, null), null, null), channel
        );

        msg.ack();
        msg.ack();
        msg.nack(false);
        msg.nack(false);

        verify(channel, times(1)).basicAck(anyLong(), anyBoolean());
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    public void receiverConsumeNoAck() throws Exception {
        Channel channel = connection.createChannel();
        int nbMessages = 10;

        receiver = ReactorRabbitMq.createReceiver();

        for (int $ : IntStream.range(0, 1).toArray()) {
            Flux<Delivery> flux = receiver.consumeNoAck(queue, new ConsumeOptions().overflowStrategy(
                FluxSink.OverflowStrategy.BUFFER
            ));
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
        assertNull(connection.createChannel().basicGet(queue, true));
    }

    @Test
    public void receiverConsumeAutoAck() throws Exception {
        Channel channel = connection.createChannel();
        int nbMessages = 10;

        receiver = ReactorRabbitMq.createReceiver();

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

        assertNull(connection.createChannel().basicGet(queue, true));
    }

    @Test
    public void receiverConsumeManuelAck() throws Exception {
        Channel channel = connection.createChannel();
        int nbMessages = 10;

        receiver = ReactorRabbitMq.createReceiver();

        for (int $ : IntStream.range(0, 10).toArray()) {
            Flux<AcknowledgableDelivery> flux = receiver.consumeManualAck(queue);

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

        assertNull(connection.createChannel().basicGet(queue, true));
    }

    @Test
    public void receiverConsumeManuelAckOverflowMessagesRequeued() throws Exception {
        // Downstream would request only one message and the hook before emission
        // would nack/requeue messages.
        // Messages are then redelivered, so a nack can fail because
        // the channel is closed (the subscription is cancelled once the 20
        // published messages have been acked (first one) and at least 19 have
        // been nacked. This can lead to some stack trace in the console, but
        // it's normal behavior.
        // This can be an example of trying no to loose messages and requeue
        // messages when the downstream consumers are overloaded
        Channel channel = connection.createChannel();
        int nbMessages = 10;

        receiver = ReactorRabbitMq.createReceiver();

        CountDownLatch ackedNackedLatch = new CountDownLatch(2 * nbMessages - 1);

        Flux<AcknowledgableDelivery> flux = receiver.consumeManualAck(queue, new ConsumeOptions()
            .overflowStrategy(FluxSink.OverflowStrategy.DROP)
            .hookBeforeEmitBiFunction((requestedFromDownstream, message) -> {
                if(requestedFromDownstream == 0) {
                    ((AcknowledgableDelivery) message).nack(true);
                    ackedNackedLatch.countDown();
                    return false;
                } else {
                    return true;
                }
            })
            .qos(1)
        );

        for (int $$ : IntStream.range(0, nbMessages).toArray()) {
            channel.basicPublish("", queue, null, "Hello".getBytes());
        }

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger counter = new AtomicInteger();
        AtomicReference<Subscription> subscriptionReference = new AtomicReference<>();
        flux.subscribe(new BaseSubscriber<AcknowledgableDelivery>() {

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                subscription.request(1);
                subscriptionReference.set(subscription);
            }

            @Override
            protected void hookOnNext(AcknowledgableDelivery message) {
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                counter.addAndGet(1);
                message.ack();
                latch.countDown();
                subscriptionReference.get().request(0);
            }
        });

        for (int $$ : IntStream.range(0, nbMessages).toArray()) {
            channel.basicPublish("", queue, null, "Hello".getBytes());
        }

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertTrue(ackedNackedLatch.await(1, TimeUnit.SECONDS));
        subscriptionReference.get().cancel();
        assertEquals(1, counter.get());
        assertTrue(connection.createChannel().queueDeclarePassive(queue).getMessageCount() > 0);
    }

    @Test
    public void receiverConsumeManuelAckOverflowMessagesDropped() throws Exception {
        // downstream would request only one message and the hook before emission
        // would ack other messages.
        // This can be an example of controlling back pressure by dropping
        // messages (because they're non-essential) with RabbitMQ QoS+ack and
        // reactor feedback from downstream.
        Channel channel = connection.createChannel();
        int nbMessages = 10;

        receiver = ReactorRabbitMq.createReceiver();

        CountDownLatch ackedDroppedLatch = new CountDownLatch(2 * nbMessages - 1);

        Flux<AcknowledgableDelivery> flux = receiver.consumeManualAck(queue, new ConsumeOptions()
            .overflowStrategy(FluxSink.OverflowStrategy.DROP)
            .hookBeforeEmitBiFunction((requestedFromDownstream, message) -> {
                if(requestedFromDownstream == 0) {
                    ((AcknowledgableDelivery) message).ack();
                    ackedDroppedLatch.countDown();
                }
                // we can emit, the message will be dropped by the overflow strategy
                return true;
            })
            .qos(1)
        );

        for (int $$ : IntStream.range(0, nbMessages).toArray()) {
            channel.basicPublish("", queue, null, "Hello".getBytes());
        }

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger counter = new AtomicInteger();
        AtomicReference<Subscription> subscriptionReference = new AtomicReference<>();
        flux.subscribe(new BaseSubscriber<AcknowledgableDelivery>() {

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                subscription.request(1);
                subscriptionReference.set(subscription);
            }

            @Override
            protected void hookOnNext(AcknowledgableDelivery message) {
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                counter.addAndGet(1);
                message.ack();
                latch.countDown();
                subscriptionReference.get().request(0);
            }
        });

        for (int $$ : IntStream.range(0, nbMessages).toArray()) {
            channel.basicPublish("", queue, null, "Hello".getBytes());
        }

        assertTrue(ackedDroppedLatch.await(1, TimeUnit.SECONDS));
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        subscriptionReference.get().cancel();
        assertEquals(1, counter.get());
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

        sender = createSender();
        sender.send(msgFlux).subscribe();
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals(nbMessages, counter.get());
    }

    @Test
    public void publishConfirms() throws Exception {
        int nbMessages = 10;
        CountDownLatch consumedLatch = new CountDownLatch(nbMessages);
        CountDownLatch confirmedLatch = new CountDownLatch(nbMessages);
        AtomicInteger counter = new AtomicInteger();
        Channel channel = connection.createChannel();
        channel.basicConsume(queue, true, (consumerTag, delivery) -> {
            counter.incrementAndGet();
            consumedLatch.countDown();
        },  consumerTag -> {});

        Flux<OutboundMessage> msgFlux = Flux.range(0, nbMessages).map(i -> new OutboundMessage("", queue, "".getBytes()));

        sender = createSender();
        sender.sendWithPublishConfirms(msgFlux).subscribe(outboundMessageResult -> confirmedLatch.countDown());

        assertTrue(consumedLatch.await(1, TimeUnit.SECONDS));
        assertTrue(confirmedLatch.await(1, TimeUnit.SECONDS));
        assertEquals(nbMessages, counter.get());
    }

    @Test
    public void publishConfirmsErrorWhilePublishing() throws Exception {
        ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
        Connection mockConnection = mock(Connection.class);
        Channel mockChannel = mock(Channel.class);
        when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
        when(mockConnection.createChannel()).thenReturn(mockChannel);

        AtomicLong publishSequence = new AtomicLong();
        when(mockChannel.getNextPublishSeqNo()).thenAnswer(invocation -> publishSequence.incrementAndGet());
        when(mockChannel.isOpen()).thenReturn(true);

        doNothing()
            .doThrow(new IOException("simulated error while publishing"))
            .when(mockChannel).basicPublish(anyString(), anyString(), any(AMQP.BasicProperties.class), any(byte[].class));

        int nbMessages = 10;
        Flux<OutboundMessage> msgFlux = Flux.range(0, nbMessages).map(i -> new OutboundMessage("", queue, "".getBytes()));
        int nbMessagesAckNack = 2;
        CountDownLatch confirmLatch = new CountDownLatch(nbMessagesAckNack);
        sender = createSender(new SenderOptions().connectionFactory(mockConnectionFactory));
        CountDownLatch subscriptionLatch = new CountDownLatch(1);
        sender.sendWithPublishConfirms(msgFlux)
            .subscribe(outboundMessageResult -> confirmLatch.countDown(),
                error -> { });

        // have to wait a bit the subscription propagates and add the confirm listener
        Thread.sleep(100L);

        ArgumentCaptor<ConfirmListener> confirmListenerArgumentCaptor = ArgumentCaptor.forClass(ConfirmListener.class);
        verify(mockChannel).addConfirmListener(confirmListenerArgumentCaptor.capture());
        ConfirmListener confirmListener = confirmListenerArgumentCaptor.getValue();

        ExecutorService ioExecutor = Executors.newSingleThreadExecutor();
        ioExecutor.submit(() -> {
            confirmListener.handleAck(1, false);
            return null;
        });

        assertTrue(confirmLatch.await(1L, TimeUnit.SECONDS));
        verify(mockChannel, times(1)).close();
    }

    @Test
    public void declareDeleteResources() throws Exception {
        Channel channel = connection.createChannel();

        final String queueName = UUID.randomUUID().toString();
        final String exchangeName = UUID.randomUUID().toString();

        try {
            CountDownLatch latchCreation = new CountDownLatch(1);
            sender = createSender();
            Disposable resourceCreation = sender.declare(exchange(exchangeName))
                .then(sender.declare(queue(queueName)))
                .then(sender.bind(binding(exchangeName, "a.b", queueName)))
                .doAfterTerminate(() -> latchCreation.countDown())
                .subscribe();

            assertTrue(latchCreation.await(1, TimeUnit.SECONDS));

            channel.exchangeDeclarePassive(exchangeName);
            channel.queueDeclarePassive(queueName);
            resourceCreation.dispose();

            CountDownLatch latchDeletion = new CountDownLatch(1);

            Disposable resourceDeletion = sender.unbind(binding(exchangeName, "a.b", queueName))
                .then(sender.delete(exchange(exchangeName)))
                .then(sender.delete(queue(queueName)))
                .doAfterTerminate(() -> latchDeletion.countDown())
                .subscribe();

            assertTrue(latchDeletion.await(1, TimeUnit.SECONDS));
            resourceDeletion.dispose();

        } finally {
            try {
                channel.exchangeDeclarePassive(exchangeName);
                fail("The exchange should have been deleted, exchangeDeclarePassive should have thrown an exception");
            } catch (IOException e) {
                // OK
                channel = connection.createChannel();
            }
            try {
                channel.queueDeclarePassive(queueName);
                fail("The QUEUE should have been deleted, queueDeclarePassive should have thrown an exception");
            } catch (IOException e) {
                // OK
            }
        }
    }

    @Test
    public void createResourcesPublishConsume() throws Exception {
        final String queueName = UUID.randomUUID().toString();
        final String exchangeName = UUID.randomUUID().toString();
        final String routingKey = "a.b";
        int nbMessages = 100;
        try {
            sender = createSender();
            receiver = ReactorRabbitMq.createReceiver();

            CountDownLatch latch = new CountDownLatch(nbMessages);
            AtomicInteger count = new AtomicInteger();

            Disposable resourceSendingConsuming = sender.declare(exchange(exchangeName))
                .then(sender.declare(queue(queueName)))
                .then(sender.bind(binding(exchangeName, routingKey, queueName)))
                .thenMany(sender.send(Flux.range(0, nbMessages)
                    .map(i -> new OutboundMessage(exchangeName, routingKey, i.toString().getBytes()))))
                .thenMany(receiver.consumeNoAck(
                    queueName,
                    new ConsumeOptions().stopConsumingBiFunction((emitter, msg) -> Integer.parseInt(new String(msg.getBody())) == nbMessages - 1)))
                .subscribe(msg -> {
                    count.incrementAndGet();
                    latch.countDown();
                });

            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertEquals(nbMessages, count.get());
            resourceSendingConsuming.dispose();
        } finally {
            final Channel channel = connection.createChannel();
            channel.exchangeDelete(exchangeName);
            channel.queueDelete(queueName);
            channel.close();
        }
    }

    @Test
    public void shovel() throws Exception {
        final String sourceQueue = UUID.randomUUID().toString();
        final String destinationQueue = UUID.randomUUID().toString();
        try {
            sender = createSender();
            receiver = ReactorRabbitMq.createReceiver();
            Mono<AMQP.Queue.DeclareOk> resources = sender.declare(queue(sourceQueue))
                .then(sender.declare(queue(destinationQueue)));

            int nbMessages = 100;

            AtomicInteger counter = new AtomicInteger();
            CountDownLatch latch = new CountDownLatch(nbMessages);

            Disposable shovel = resources.then(sender.send(Flux.range(0, nbMessages).map
                (i -> new OutboundMessage("", sourceQueue, i.toString().getBytes()))))
                .thenMany(receiver.consumeNoAck(
                        sourceQueue,
                        new ConsumeOptions().stopConsumingBiFunction((emitter, msg) -> Integer.parseInt(new String(msg.getBody())) == nbMessages - 1)
                    ).map(delivery -> new OutboundMessage("", destinationQueue, delivery.getBody()))
                     .transform(messages -> sender.send(messages)))
                .thenMany(receiver.consumeNoAck(destinationQueue)).subscribe(msg -> {
                    counter.incrementAndGet();
                    latch.countDown();
                });

            assertTrue(latch.await(3, TimeUnit.SECONDS));
            assertEquals(nbMessages, counter.get());
            shovel.dispose();
        } finally {
            Channel channel = connection.createChannel();
            channel.queueDelete(sourceQueue);
            channel.queueDelete(destinationQueue);
        }
    }

    @Test public void partitions() throws Exception {
        sender = createSender();
        receiver = ReactorRabbitMq.createReceiver();
        int nbPartitions = 4;
        int nbMessages = 100;

        Flux<Tuple2<Integer, String>> queues = Flux.range(0, nbPartitions)
            .flatMap(partition -> sender.declare(queue("partitions"+partition).autoDelete(true))
                                                .map(q -> Tuples.of(partition, q.getQueue())))
            .cache();

        Flux<AMQP.Queue.BindOk> bindings = queues
            .flatMap(q -> sender.bind(binding("amq.direct", "" + q.getT1(), q.getT2())));

        Flux<OutboundMessage> messages = Flux.range(0, nbMessages).groupBy(v -> v % nbPartitions)
            .flatMap(partitionFlux -> partitionFlux.publishOn(Schedulers.elastic())
                .map(v -> new OutboundMessage("amq.direct", partitionFlux.key().toString(), (v+"").getBytes())));

        CountDownLatch latch = new CountDownLatch(nbMessages);

        Disposable flow = bindings
            .then(sender.send(messages))
            .thenMany(queues.flatMap(q -> receiver.consumeAutoAck(q.getT2())))
            .subscribe(msg -> latch.countDown());

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        flow.dispose();
    }

    @Test public void connectionMonoSharedConnection() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        Mono<? extends Connection> connectionMono = Utils.singleConnectionMono(connectionFactory, cf -> cf.newConnection());

        sender = createSender(new SenderOptions().connectionMono(connectionMono));
        receiver = createReceiver(new ReceiverOptions().connectionMono(connectionMono));

        String connectionQueue = sender.declare(QueueSpecification.queue().durable(false).autoDelete(true).exclusive(true))
            .block().getQueue();

        sendAndReceiveMessages(connectionQueue);
    }

    @Test public void creatConnectionWithConnectionSupplier() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();

        sender = createSender(new SenderOptions()
            .connectionFactory(connectionFactory)
            .connectionSupplier(cf -> cf.newConnection(
                "reactive-sender")
            )
        );

        receiver = createReceiver(new ReceiverOptions()
            .connectionFactory(connectionFactory)
            .connectionSupplier(cf -> cf.newConnection("reactive-receiver"))
        );

        sendAndReceiveMessages(queue);
    }

    @Test public void createConnectionWithConnectionMono() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();

        sender = createSender(new SenderOptions()
            .connectionMono(Mono.fromCallable(() -> connectionFactory.newConnection("reactive-sender")))
        );

        receiver = createReceiver(new ReceiverOptions()
            .connectionMono(Mono.fromCallable(() -> connectionFactory.newConnection("reactive-receiver")))
        );

        sendAndReceiveMessages(queue);
    }

    private void sendAndReceiveMessages(String queue) throws Exception {
        int nbMessages = 10;
        CountDownLatch latch = new CountDownLatch(nbMessages);
        Disposable subscriber = receiver.consumeAutoAck(queue).subscribe(delivery -> latch.countDown());

        Flux<OutboundMessage> msgFlux = Flux.range(0, nbMessages).map(i -> new OutboundMessage("", queue, "".getBytes()));
        sender.send(msgFlux).subscribe();

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        subscriber.dispose();
    }
}
