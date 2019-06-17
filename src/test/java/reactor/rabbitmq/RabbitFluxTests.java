/*
 * Copyright (c) 2017-2019 Pivotal Software Inc, All Rights Reserved.
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.*;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.ChannelCloseHandlers.SenderChannelCloseHandler;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.of;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;
import static reactor.rabbitmq.RabbitFlux.createReceiver;
import static reactor.rabbitmq.RabbitFlux.createSender;
import static reactor.rabbitmq.ResourcesSpecification.*;

/**
 *
 */
public class RabbitFluxTests {

    // TODO refactor test with StepVerifier

    Connection connection;
    String queue;

    Receiver receiver;
    Sender sender;

    static Stream<Arguments> noAckAndManualAckFluxArguments() {
        return Stream.of(
                of((BiFunction<Receiver, String, Flux<Delivery>>) (receiver, queue) -> receiver.consumeNoAck(queue)),
                of((BiFunction<Receiver, String, Flux<? extends Delivery>>) (receiver, queue) -> receiver.consumeManualAck(queue))
        );
    }

    public static Object[][] senderWithCustomChannelCloseHandlerPriorityArguments() {
        return new Object[][]{
                new Object[]{10, (Function<Tuple3<Sender, Publisher<OutboundMessage>, SendOptions>, Publisher>) objects -> objects.getT1().send(objects.getT2(), objects.getT3()), 0},
                new Object[]{10, (Function<Tuple3<Sender, Publisher<OutboundMessage>, SendOptions>, Publisher>) objects -> objects.getT1().sendWithPublishConfirms(objects.getT2(), objects.getT3()), 10}
        };
    }

    static Collection<ChannelOperation> declarePassiveArguments() {
        return Arrays.asList(
                (channel, ctx) -> {
                },
                (channel, ctx) -> channel.queueDeclare(ctx.toString(), false, false, false, null)
        );
    }

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

    @Test
    public void senderCloseIsIdempotent() {
        sender = createSender();

        sender.send(Flux.just(new OutboundMessage("", "dummy", new byte[0])))
                .then().block();

        sender.close();
        sender.close();
    }

    @Test
    public void receiverCloseIsIdempotent() throws Exception {
        receiver = RabbitFlux.createReceiver();

        sender = createSender();
        sender.send(Flux.just(new OutboundMessage("", queue, new byte[0]))).block();

        CountDownLatch latch = new CountDownLatch(1);
        Disposable subscribe = receiver.consumeAutoAck(queue).subscribe(delivery -> latch.countDown());

        assertTrue(latch.await(5, TimeUnit.SECONDS), "A message should have been received");
        subscribe.dispose();

        receiver.close();
        receiver.close();
    }

    @Test
    public void acknowledgableDeliveryAckNackIsIdempotent() throws Exception {
        Channel channel = mock(Channel.class);
        BiConsumer<Receiver.AcknowledgmentContext, Exception> exceptionHandler = mock(ExceptionHandlers.RetryAcknowledgmentExceptionHandler.class);
        doNothing().doThrow(new IOException()).when(channel).basicAck(anyLong(), anyBoolean());
        doThrow(new IOException()).when(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
        AcknowledgableDelivery msg = new AcknowledgableDelivery(
                new Delivery(new Envelope(0, true, null, null), null, null), channel, exceptionHandler
        );

        msg.ack();
        msg.ack();
        msg.nack(false);
        msg.nack(false);

        verify(channel, times(1)).basicAck(anyLong(), anyBoolean());
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
        verify(exceptionHandler, never()).accept(any(Receiver.AcknowledgmentContext.class), any(Exception.class));
    }

    @Test
    public void acknowledgableDeliveryWithSuccessfulRetry() throws Exception {
        Channel channel = mock(Channel.class);
        BiConsumer<Receiver.AcknowledgmentContext, Exception> exceptionHandler =
                (acknowledgmentContext, e) -> acknowledgmentContext.ackOrNack();

        doThrow(new IOException()).doNothing().when(channel).basicAck(anyLong(), anyBoolean());

        AcknowledgableDelivery msg = new AcknowledgableDelivery(
                new Delivery(new Envelope(0, true, null, null), null, null), channel, exceptionHandler
        );

        msg.ack();

        verify(channel, times(2)).basicAck(anyLong(), anyBoolean());
    }

    @Test
    public void acknowledgableDeliveryWithUnsuccessfulRetry() throws Exception {
        Channel channel = mock(Channel.class);
        BiConsumer<Receiver.AcknowledgmentContext, Exception> exceptionHandler =
                (acknowledgmentContext, e) -> acknowledgmentContext.ackOrNack();

        doThrow(new RuntimeException("exc")).when(channel).basicAck(anyLong(), anyBoolean());

        AcknowledgableDelivery msg = new AcknowledgableDelivery(
                new Delivery(new Envelope(0, true, null, null), null, null), channel, exceptionHandler
        );

        assertThatThrownBy(msg::ack).hasMessage("exc");

        verify(channel, times(2)).basicAck(anyLong(), anyBoolean());
    }

    @Test
    public void receiverConsumeNoAck() throws Exception {
        Channel channel = connection.createChannel();
        int nbMessages = 10;

        receiver = RabbitFlux.createReceiver();

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

        receiver = RabbitFlux.createReceiver();

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

        receiver = RabbitFlux.createReceiver();

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

        receiver = RabbitFlux.createReceiver();

        CountDownLatch ackedNackedLatch = new CountDownLatch(2 * nbMessages - 1);

        Flux<AcknowledgableDelivery> flux = receiver.consumeManualAck(queue, new ConsumeOptions()
                .overflowStrategy(FluxSink.OverflowStrategy.DROP)
                .hookBeforeEmitBiFunction((requestedFromDownstream, message) -> {
                    if (requestedFromDownstream == 0) {
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

        receiver = RabbitFlux.createReceiver();

        CountDownLatch ackedDroppedLatch = new CountDownLatch(2 * nbMessages - 1);

        Flux<AcknowledgableDelivery> flux = receiver.consumeManualAck(queue, new ConsumeOptions()
                .overflowStrategy(FluxSink.OverflowStrategy.DROP)
                .hookBeforeEmitBiFunction((requestedFromDownstream, message) -> {
                    if (requestedFromDownstream == 0) {
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

    @ParameterizedTest
    @MethodSource("noAckAndManualAckFluxArguments")
    public void receiverQueueDeleted(BiFunction<Receiver, String, Flux<? extends Delivery>> fluxFactory) throws Exception {
        // given
        Channel channel = connection.createChannel();
        Channel channelSpy = spy(channel);
        Connection mockConnection = mock(Connection.class);
        when(mockConnection.createChannel()).thenReturn(channelSpy);

        CountDownLatch latch = new CountDownLatch(1);
        channel.basicPublish("", queue, null, "Hello".getBytes());
        receiver = RabbitFlux.createReceiver(new ReceiverOptions().connectionMono(Mono.just(mockConnection)));

        fluxFactory.apply(receiver, queue).subscribe(delivery -> {
            latch.countDown();
        });

        assertTrue(latch.await(1, TimeUnit.SECONDS));

        // when
        channel.queueDelete(queue); // calls CancelCallback, consumerTag is unknown

        // then
        verify(channelSpy, never()).basicCancel(anyString());
    }

    @ParameterizedTest
    @MethodSource("noAckAndManualAckFluxArguments")
    public void receiverOnDispose(BiFunction<Receiver, String, Flux<? extends Delivery>> fluxFactory) throws Exception {
        // given
        Channel channel = connection.createChannel();
        Channel channelSpy = spy(channel);
        Connection mockConnection = mock(Connection.class);
        when(mockConnection.createChannel()).thenReturn(channelSpy);

        CountDownLatch latch = new CountDownLatch(1);
        channel.basicPublish("", queue, null, "Hello".getBytes());
        receiver = RabbitFlux.createReceiver(new ReceiverOptions().connectionMono(Mono.just(mockConnection)));

        Disposable subscription = fluxFactory.apply(receiver, queue).subscribe(delivery -> {
            latch.countDown();
        });
        assertTrue(latch.await(1, TimeUnit.SECONDS));

        // when
        subscription.dispose();

        // then
        verify(channelSpy).basicCancel(anyString());
    }

    @ParameterizedTest
    @MethodSource("noAckAndManualAckFluxArguments")
    public void receiverErrorHandling(BiFunction<Receiver, String, Flux<? extends Delivery>> fluxFactory) {
        Mono<Connection> connectionMono = Mono.fromCallable(() -> {
            throw new RuntimeException();
        });
        receiver = RabbitFlux.createReceiver(new ReceiverOptions().connectionMono(connectionMono));
        Flux<? extends Delivery> flux = fluxFactory.apply(receiver, queue);
        AtomicBoolean errorHandlerCalled = new AtomicBoolean(false);
        Disposable disposable = flux.subscribe(delivery -> {
        }, error -> errorHandlerCalled.set(true));
        assertTrue(errorHandlerCalled.get());
        disposable.dispose();
    }

    @ParameterizedTest
    @MethodSource("noAckAndManualAckFluxArguments")
    public void receiverFluxDisposedOnConnectionClose(BiFunction<Receiver, String, Flux<? extends Delivery>> fluxFactory) throws Exception {
        Channel channel = connection.createChannel();
        int nbMessages = 10;
        Mono<Connection> connectionMono = Mono.fromCallable(() -> {
            ConnectionFactory cf = new ConnectionFactory();
            cf.useNio();
            return cf.newConnection();
        }).cache();
        receiver = RabbitFlux.createReceiver(new ReceiverOptions().connectionMono(connectionMono));

        Flux<? extends Delivery> flux = fluxFactory.apply(receiver, queue);
        for (int $$ : IntStream.range(0, nbMessages).toArray()) {
            channel.basicPublish("", queue, null, "Hello".getBytes());
        }

        CountDownLatch messageReceivedLatch = new CountDownLatch(nbMessages);
        CountDownLatch completedLatch = new CountDownLatch(1);
        AtomicInteger counter = new AtomicInteger();
        Disposable subscription = flux.subscribe(msg -> {
            counter.incrementAndGet();
            messageReceivedLatch.countDown();
        }, error -> {
        }, () -> completedLatch.countDown());

        assertTrue(messageReceivedLatch.await(1, TimeUnit.SECONDS));
        assertEquals(nbMessages, counter.get());
        assertEquals(1, completedLatch.getCount());
        connectionMono.block().close();
        assertTrue(completedLatch.await(1, TimeUnit.SECONDS));
        subscription.dispose();
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
    public void senderRetryCreateChannel() throws Exception {
        ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
        Connection mockConnection = mock(Connection.class);
        when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
        when(mockConnection.createChannel())
                .thenThrow(new IOException("already closed exception"))
                .thenThrow(new IOException("already closed exception"))
                .thenReturn(connection.createChannel());

        int nbMessages = 10;

        Flux<OutboundMessage> msgFlux = Flux.range(0, nbMessages).map(i -> new OutboundMessage("", queue, "".getBytes()));

        sender = createSender(new SenderOptions().connectionFactory(mockConnectionFactory));

        StepVerifier.create(sender.send(msgFlux).retry(2))
                .verifyComplete();
        verify(mockConnection, times(3)).createChannel();

        StepVerifier.create(consume(queue, nbMessages))
                .expectNextCount(nbMessages)
                .verifyComplete();
    }

    @Test
    public void senderRetryNotWorkingWhenCreateChannelIsCached() throws Exception {
        int nbMessages = 10;

        Connection mockConnection = mock(Connection.class);
        Channel mockChannel = mock(Channel.class);
        when(mockConnection.createChannel())
                .thenThrow(new RuntimeException("already closed exception"))
                .thenThrow(new RuntimeException("already closed exception"))
                .thenReturn(mockChannel);

        Flux<OutboundMessage> msgFlux = Flux.range(0, nbMessages).map(i -> new OutboundMessage("", queue, "".getBytes()));

        SenderOptions senderOptions = new SenderOptions()
                .channelMono(Mono.just(mockConnection).map(this::createChannel).cache());

        sender = createSender(senderOptions);

        StepVerifier.create(sender.send(msgFlux).retry(2))
                .expectError(RabbitFluxException.class)
                .verify();

        verify(mockChannel, never()).basicPublish(anyString(), anyString(), any(AMQP.BasicProperties.class), any(byte[].class));
        verify(mockChannel, never()).close();
    }

    @Test
    public void senderWithCustomChannelCloseHandler() throws Exception {
        int nbMessages = 10;
        Flux<OutboundMessage> msgFlux = Flux.range(0, nbMessages).map(i -> new OutboundMessage("", queue, "".getBytes()));

        SenderChannelCloseHandler channelCloseHandler = mock(SenderChannelCloseHandler.class);
        doNothing().when(channelCloseHandler).accept(any(SignalType.class), any(Channel.class));
        Mono<Channel> monoChannel = Mono.fromCallable(() -> connection.createChannel()).cache();
        SenderOptions senderOptions = new SenderOptions().channelCloseHandler(channelCloseHandler).channelMono(monoChannel);

        sender = createSender(senderOptions);

        Mono<Void> sendTwice = Mono.when(sender.send(msgFlux), sender.send(msgFlux))
                .doFinally(signalType -> {
                    try {
                        monoChannel.block().close();
                    } catch (Exception e) {
                        throw new RabbitFluxException(e);
                    }
                });

        StepVerifier.create(sendTwice)
                .verifyComplete();
        verify(channelCloseHandler, times(2)).accept(SignalType.ON_COMPLETE, monoChannel.block());

        StepVerifier.create(consume(queue, nbMessages * 2))
                .expectNextCount(nbMessages * 2)
                .verifyComplete();
    }

    @ParameterizedTest
    @MethodSource("senderWithCustomChannelCloseHandlerPriorityArguments")
    public void senderWithCustomChannelCloseHandlerPriority(int nbMessages,
                                                            Function<Tuple3<Sender, Publisher<OutboundMessage>, SendOptions>, Publisher> sendingCallback,
                                                            int expectedCount) throws InterruptedException {
        Flux<OutboundMessage> msgFlux = Flux.range(0, nbMessages).map(i -> new OutboundMessage("", queue, "".getBytes()));

        SenderChannelCloseHandler channelCloseHandlerInSenderOptions = mock(SenderChannelCloseHandler.class);
        SenderChannelCloseHandler channelCloseHandlerInSendOptions = mock(SenderChannelCloseHandler.class);

        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(answer -> {
            latch.countDown();
            return null;
        }).when(channelCloseHandlerInSendOptions).accept(any(SignalType.class), any(Channel.class));

        SenderOptions senderOptions = new SenderOptions().channelCloseHandler(channelCloseHandlerInSenderOptions);
        sender = createSender(senderOptions);
        SendOptions sendOptions = new SendOptions().channelCloseHandler(channelCloseHandlerInSendOptions);

        Publisher<?> sendingResult = sendingCallback.apply(Tuples.of(sender, msgFlux, sendOptions));

        StepVerifier.create(sendingResult)
                .expectNextCount(expectedCount)
                .verifyComplete();

        assertTrue(latch.await(1, TimeUnit.SECONDS));

        verify(channelCloseHandlerInSenderOptions, never()).accept(any(SignalType.class), any(Channel.class));
        verify(channelCloseHandlerInSendOptions, times(1)).accept(any(SignalType.class), any(Channel.class));
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
        }, consumerTag -> {
        });

        Flux<OutboundMessage> msgFlux = Flux.range(0, nbMessages).map(i -> new OutboundMessage("", queue, "".getBytes()));

        sender = createSender();
        sender.sendWithPublishConfirms(msgFlux).subscribe(outboundMessageResult -> {
            if (outboundMessageResult.isAck() && outboundMessageResult.getOutboundMessage() != null) {
                confirmedLatch.countDown();
            }
        });

        assertTrue(consumedLatch.await(1, TimeUnit.SECONDS));
        assertTrue(confirmedLatch.await(1, TimeUnit.SECONDS));
        assertEquals(nbMessages, counter.get());
    }

    @Test
    public void publishConfirmsBackpressure() throws Exception {
        int nbMessages = 10;
        int subscriberRequest = 3;
        CountDownLatch consumedLatch = new CountDownLatch(subscriberRequest);
        CountDownLatch confirmedLatch = new CountDownLatch(subscriberRequest);
        AtomicInteger counter = new AtomicInteger();
        Channel channel = connection.createChannel();
        channel.basicConsume(queue, true, (consumerTag, delivery) -> {
            counter.incrementAndGet();
            consumedLatch.countDown();
        }, consumerTag -> {
        });

        Flux<OutboundMessage> msgFlux = Flux.range(0, nbMessages).map(i -> new OutboundMessage("", queue, "".getBytes()));

        sender = createSender();
        sender.sendWithPublishConfirms(msgFlux).subscribe(new BaseSubscriber<OutboundMessageResult>() {
            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                subscription.request(subscriberRequest);
            }

            @Override
            protected void hookOnNext(OutboundMessageResult outboundMessageResult) {
                if (outboundMessageResult.getOutboundMessage() != null) {
                    confirmedLatch.countDown();
                }
            }
        });

        assertTrue(consumedLatch.await(1, TimeUnit.SECONDS));
        assertTrue(confirmedLatch.await(1, TimeUnit.SECONDS));
        assertEquals(subscriberRequest, counter.get());
    }

    @Test
    public void publishConfirmsEmptyPublisher() throws Exception {
        CountDownLatch finallyLatch = new CountDownLatch(1);
        Flux<OutboundMessage> msgFlux = Flux.empty();

        sender = createSender();
        sender.sendWithPublishConfirms(msgFlux)
                .doFinally(signalType -> finallyLatch.countDown())
                .subscribe();

        assertTrue(finallyLatch.await(1, TimeUnit.SECONDS));
    }

    @Test
    void publishConfirmsMaxInFlight() throws InterruptedException {
        int maxConcurrency = 4;
        int nbMessages = 100;
        CountDownLatch confirmedLatch = new CountDownLatch(nbMessages);

        AtomicInteger inflight = new AtomicInteger();
        AtomicInteger maxInflight = new AtomicInteger();

        Flux<OutboundMessage> msgFlux = Flux.range(0, nbMessages).map(i -> {
            int current = inflight.incrementAndGet();
            if (current > maxInflight.get()) {
                maxInflight.set(current);
            }
            return new OutboundMessage("", queue, "".getBytes());
        });

        sender = createSender();
        sender
                .sendWithPublishConfirms(msgFlux, new SendOptions().maxInFlight(maxConcurrency))
                .subscribe(outboundMessageResult -> {
                    inflight.decrementAndGet();
                    if (outboundMessageResult.isAck() && outboundMessageResult.getOutboundMessage() != null) {
                        confirmedLatch.countDown();
                    }
                });

        assertTrue(confirmedLatch.await(1, TimeUnit.SECONDS));
        assertThat(maxInflight.get()).isLessThanOrEqualTo(maxConcurrency);
    }

    @Test
    public void publishConfirmsErrorWhilePublishing() throws Exception {
        ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
        Connection mockConnection = mock(Connection.class);
        Channel mockChannel = mock(Channel.class);
        when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
        when(mockConnection.createChannel()).thenReturn(mockChannel);
        when(mockConnection.isOpen()).thenReturn(true);
        when(mockChannel.getConnection()).thenReturn(mockConnection);

        AtomicLong publishSequence = new AtomicLong();
        when(mockChannel.getNextPublishSeqNo()).thenAnswer(invocation -> publishSequence.incrementAndGet());
        when(mockChannel.isOpen()).thenReturn(true);

        CountDownLatch channelCloseLatch = new CountDownLatch(1);
        doAnswer(answer -> {
            channelCloseLatch.countDown();
            return null;
        }).when(mockChannel).close();

        CountDownLatch serverPublishConfirmLatch = new CountDownLatch(1);
        doNothing()
                .doAnswer(answer -> {
                    // see https://github.com/reactor/reactor-rabbitmq/pull/67#issuecomment-472789735
                    serverPublishConfirmLatch.await(5, TimeUnit.SECONDS);
                    throw new IOException("simulated error while publishing");
                })
                .when(mockChannel).basicPublish(anyString(), anyString(), nullable(AMQP.BasicProperties.class), any(byte[].class));


        int nbMessages = 10;
        Flux<OutboundMessage> msgFlux = Flux.range(0, nbMessages).map(i -> new OutboundMessage("", queue, "".getBytes()));
        int nbMessagesAckNack = 1 + 1; // first published message confirmed + "fake" confirmation because of sending failure
        CountDownLatch confirmLatch = new CountDownLatch(nbMessagesAckNack);
        sender = createSender(new SenderOptions().connectionFactory(mockConnectionFactory));
        sender.sendWithPublishConfirms(msgFlux, new SendOptions().exceptionHandler((ctx, e) -> {
            throw new RabbitFluxException(e);
        })).subscribe(outboundMessageResult -> {
                    if (outboundMessageResult.getOutboundMessage() != null) {
                        confirmLatch.countDown();
                    }
                },
                error -> {
                });

        // have to wait a bit the subscription propagates and add the confirm listener
        Thread.sleep(100L);

        ArgumentCaptor<ConfirmListener> confirmListenerArgumentCaptor = ArgumentCaptor.forClass(ConfirmListener.class);
        verify(mockChannel).addConfirmListener(confirmListenerArgumentCaptor.capture());
        ConfirmListener confirmListener = confirmListenerArgumentCaptor.getValue();

        ExecutorService ioExecutor = Executors.newSingleThreadExecutor();
        ioExecutor.submit(() -> {
            confirmListener.handleAck(1, false);
            serverPublishConfirmLatch.countDown();
            return null;
        });

        assertTrue(confirmLatch.await(1L, TimeUnit.SECONDS));
        assertTrue(channelCloseLatch.await(1L, TimeUnit.SECONDS));
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
                fail("The queue should have been deleted, queueDeclarePassive should have thrown an exception");
            } catch (IOException e) {
                // OK
            }
        }
    }

    @Test
    public void declareDeleteResourcesWithOptions() throws Exception {
        Channel channel = connection.createChannel();

        final String queueName = UUID.randomUUID().toString();
        final String exchangeName = UUID.randomUUID().toString();

        try {
            SenderOptions senderOptions = new SenderOptions();
            Mono<Connection> connectionMono = Mono.fromCallable(() -> senderOptions.getConnectionFactory().newConnection());
            AtomicInteger channelMonoCalls = new AtomicInteger(0);
            Mono<Channel> channelMono = connectionMono.map(c -> {
                try {
                    channelMonoCalls.incrementAndGet();
                    return c.createChannel();
                } catch (Exception e) {
                    Exceptions.propagate(e);
                }
                return null;
            });
            senderOptions.connectionMono(connectionMono);
            CountDownLatch latchCreation = new CountDownLatch(1);
            sender = createSender(senderOptions);

            ResourceManagementOptions options = new ResourceManagementOptions()
                    .channelMono(channelMono);

            Disposable resourceCreation = sender.declare(exchange(exchangeName), options)
                    .then(sender.declare(queue(queueName), options))
                    .then(sender.bind(binding(exchangeName, "a.b", queueName), options))
                    .doAfterTerminate(() -> latchCreation.countDown())
                    .subscribe();

            assertTrue(latchCreation.await(1, TimeUnit.SECONDS));
            assertEquals(3, channelMonoCalls.get());

            channel.exchangeDeclarePassive(exchangeName);
            channel.queueDeclarePassive(queueName);
            resourceCreation.dispose();

            CountDownLatch latchDeletion = new CountDownLatch(1);

            Disposable resourceDeletion = sender.unbind(binding(exchangeName, "a.b", queueName), options)
                    .then(sender.delete(exchange(exchangeName), options))
                    .then(sender.delete(queue(queueName), options))
                    .doAfterTerminate(() -> latchDeletion.countDown())
                    .subscribe();

            assertTrue(latchDeletion.await(1, TimeUnit.SECONDS));
            assertEquals(3 + 3, channelMonoCalls.get());
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
                fail("The queue should have been deleted, queueDeclarePassive should have thrown an exception");
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
            receiver = RabbitFlux.createReceiver();

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

    @ParameterizedTest
    @MethodSource("declarePassiveArguments")
    public void declarePassive(ChannelOperation channelOperation) throws Exception {
        Channel channel = connection.createChannel();
        final String queue = UUID.randomUUID().toString();
        try {
            channelOperation.doWithChannel(channel, queue);
            CountDownLatch creationLatch = new CountDownLatch(1);
            int messageCount = 10;
            Flux<OutboundMessage> messages = Flux.range(0, messageCount)
                    .map(i -> new OutboundMessage("", queue, "".getBytes()));
            sender = createSender();
            sender.declare(queue(queue).passive(true))
                    .onErrorResume(exception -> sender.declare(queue(queue)))
                    .then(sender.send(messages))
                    .doFinally($$ -> creationLatch.countDown())
                    .subscribe();

            assertThat(creationLatch.await(5, TimeUnit.SECONDS)).isTrue();

            CountDownLatch messagesLatch = new CountDownLatch(messageCount);
            channel.basicConsume(queue, (consumerTag, message) -> messagesLatch.countDown(), ctag -> {
            });
            assertThat(messagesLatch.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            channel.queueDelete(queue);
        }
    }

    @Test public void emitErrorOnPublisherConfWhenChannelIsClosedByServer() throws Exception {
        String nonExistingExchange = UUID.randomUUID().toString();
        int messageCount = 5;
        Flux<OutboundMessage> msgFlux = Flux.range(0, messageCount)
                .map(i -> new OutboundMessage(nonExistingExchange, queue, "".getBytes()));

        CountDownLatch latch = new CountDownLatch(1);
        sender = createSender();
        sender.sendWithPublishConfirms(msgFlux).doOnError(e -> {
            if (e instanceof ShutdownSignalException) {
                latch.countDown();
            }
        }).subscribe();
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void shovel() throws Exception {
        final String sourceQueue = UUID.randomUUID().toString();
        final String destinationQueue = UUID.randomUUID().toString();
        try {
            sender = createSender();
            receiver = RabbitFlux.createReceiver();
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

    @Test
    public void partitions() throws Exception {
        sender = createSender();
        receiver = RabbitFlux.createReceiver();
        int nbPartitions = 4;
        int nbMessages = 100;

        Flux<Tuple2<Integer, String>> queues = Flux.range(0, nbPartitions)
                .flatMap(partition -> sender.declare(queue("partitions" + partition).autoDelete(true))
                        .map(q -> Tuples.of(partition, q.getQueue())))
                .cache();

        Flux<AMQP.Queue.BindOk> bindings = queues
                .flatMap(q -> sender.bind(binding("amq.direct", "" + q.getT1(), q.getT2())));

        Flux<OutboundMessage> messages = Flux.range(0, nbMessages).groupBy(v -> v % nbPartitions)
                .flatMap(partitionFlux -> partitionFlux.publishOn(Schedulers.elastic())
                        .map(v -> new OutboundMessage("amq.direct", partitionFlux.key().toString(), (v + "").getBytes())));

        CountDownLatch latch = new CountDownLatch(nbMessages);

        Disposable flow = bindings
                .then(sender.send(messages))
                .thenMany(queues.flatMap(q -> receiver.consumeAutoAck(q.getT2())))
                .subscribe(msg -> latch.countDown());

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        flow.dispose();
    }

    @Test
    public void connectionMonoSharedConnection() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        Mono<? extends Connection> connectionMono = Utils.singleConnectionMono(connectionFactory, cf -> cf.newConnection());

        sender = createSender(new SenderOptions().connectionMono(connectionMono));
        receiver = createReceiver(new ReceiverOptions().connectionMono(connectionMono));

        String connectionQueue = sender.declare(QueueSpecification.queue().durable(false).autoDelete(true).exclusive(true))
                .block().getQueue();

        sendAndReceiveMessages(connectionQueue);
    }

    @Test
    public void creatConnectionWithConnectionSupplier() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();

        sender = createSender(new SenderOptions()
                .connectionFactory(connectionFactory)
                .connectionSupplier(cf -> cf.newConnection(
                        "reactive-sendRetryOnFailure")
                )
        );

        receiver = createReceiver(new ReceiverOptions()
                .connectionFactory(connectionFactory)
                .connectionSupplier(cf -> cf.newConnection("reactive-receiver"))
        );

        sendAndReceiveMessages(queue);
    }

    @Test
    public void createConnectionWithConnectionMono() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();

        sender = createSender(new SenderOptions()
                .connectionMono(Mono.fromCallable(() -> connectionFactory.newConnection("reactive-sendRetryOnFailure")))
        );

        receiver = createReceiver(new ReceiverOptions()
                .connectionMono(Mono.fromCallable(() -> connectionFactory.newConnection("reactive-receiver")))
        );

        sendAndReceiveMessages(queue);
    }

    @Test
    public void creatingNonExistentPassiveChannelResultsInError() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();

        sender = createSender(new SenderOptions()
                .connectionMono(Mono.fromCallable(() -> connectionFactory.newConnection("non-existing-passive-queue"))));

        StepVerifier.create(sender.declareQueue(QueueSpecification.queue("non-existing-queue").passive(true))).expectError(ShutdownSignalException.class).verify();
    }

    @Test
    public void creatingNonExistentPassiveExchangeResultsInError() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();

        sender = createSender(new SenderOptions()
                .connectionMono(Mono.fromCallable(() -> connectionFactory.newConnection("non-existing-passive-exchange"))));

        StepVerifier.create(sender.declareExchange(ExchangeSpecification.exchange("non-existing-exchange").passive(true))).expectError(ShutdownSignalException.class).verify();
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

    private Flux<Delivery> consume(final String queue, int nbMessages) throws Exception {
        return consume(queue, nbMessages, Duration.ofSeconds(1L));
    }

    private Flux<Delivery> consume(final String queue, int nbMessages, Duration timeout) throws Exception {
        Channel channel = connection.createChannel();
        Flux<Delivery> consumeFlux = Flux.create(emitter -> Mono.just(nbMessages).map(AtomicInteger::new).subscribe(countdown -> {
            DeliverCallback deliverCallback = (consumerTag, message) -> {
                emitter.next(message);
                if (countdown.decrementAndGet() <= 0) {
                    emitter.complete();
                }
            };
            CancelCallback cancelCallback = consumerTag -> {
            };
            try {
                channel.basicConsume(queue, true, deliverCallback, cancelCallback);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
        return consumeFlux.timeout(timeout);
    }

    private Channel createChannel(Connection connection) {
        try {
            return connection.createChannel();
        } catch (Exception e) {
            throw new RabbitFluxException(e);
        }
    }

    @FunctionalInterface
    interface ChannelOperation {

        void doWithChannel(Channel channel, Object context) throws Exception;

    }

}
