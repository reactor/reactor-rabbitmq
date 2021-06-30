/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
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
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
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
    void canReuseChannelOnError() {
        sender = createSender();
        try {
            sender.declare(QueueSpecification.queue(queue).autoDelete(true)).block();
            fail("Trying to re-declare queue with different arguments, should have failed");
        } catch (ShutdownSignalException e) {
            // OK
        }
        sender.declare(QueueSpecification.queue()).block();
    }

    @Test
    void channelMonoPriority() {
        Mono<Channel> senderChannelMono = Mono.just(mock(Channel.class));
        Mono<Channel> sendChannelMono = Mono.just(mock(Channel.class));
        sender = createSender();
        assertNotNull(sender.getChannelMono(new SendOptions()));
        assertSame(sendChannelMono, sender.getChannelMono(new SendOptions().channelMono(sendChannelMono)));

        sender = createSender(new SenderOptions().channelMono(senderChannelMono));
        assertSame(senderChannelMono, sender.getChannelMono(new SendOptions()));
        assertSame(sendChannelMono, sender.getChannelMono(new SendOptions().channelMono(sendChannelMono)));
    }

    @Test
    void createExchangeBeforePublishing() throws Exception {
        int nbMessages = 10;
        CountDownLatch latch = new CountDownLatch(nbMessages);
        AtomicInteger counter = new AtomicInteger();
        Channel channel = connection.createChannel();
        channel.basicConsume(queue, true, new DefaultConsumer(channel) {

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                counter.incrementAndGet();
                latch.countDown();
            }
        });

        String exchange = UUID.randomUUID().toString();

        Flux<OutboundMessage> msgFlux = Flux.range(0, nbMessages).map(i -> new OutboundMessage(exchange, queue, "".getBytes()));

        sender = createSender();
        sender.declare(ExchangeSpecification.exchange(exchange).type("direct").autoDelete(true))
                .then(sender.bind(BindingSpecification.binding(exchange, queue, queue)))
                .then(sender.send(msgFlux)).subscribe();
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals(nbMessages, counter.get());
    }

    @Test
    void connectionFromSupplierShouldBeCached() throws Exception {
        ConnectionFactory cf = new ConnectionFactory();
        cf.useNio();
        Connection c = cf.newConnection();
        AtomicInteger callToConnectionSupplierCount = new AtomicInteger(0);
        SenderOptions options = new SenderOptions();
        options.connectionSupplier(cf, connectionFactory -> {
            callToConnectionSupplierCount.incrementAndGet();
            return c;
        });

        int messageCount = 10;
        CountDownLatch latch = new CountDownLatch(messageCount * 2);
        sender = createSender(options);
        String q = sender.declare(QueueSpecification.queue()).block().getQueue();
        c.createChannel().basicConsume(q, true, ((consumerTag, message) -> latch.countDown()), (consumerTag -> {
        }));
        sender.send(Flux.range(0, messageCount)
                .map(i -> new OutboundMessage("", q, i.toString().getBytes()))).block();
        sender.send(Flux.range(0, messageCount)
                .map(i -> new OutboundMessage("", q, i.toString().getBytes()))).block();

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals(1, callToConnectionSupplierCount.get());
    }

    @ValueSource(ints = {-1, 0, 10000})
    @ParameterizedTest
    public void connectionIsClosedWithDefaultTimeoutAndOverriddenValue(int timeoutInMs) throws Exception {
        Connection c = mock(Connection.class);
        Channel ch = mock(Channel.class);
        when(c.createChannel()).thenReturn(ch);

        SenderOptions options = new SenderOptions()
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

        Sender sender = new Sender(options);

        sender.send(Flux.range(1, 10).map(i -> new OutboundMessage("", "", null))).block();

        sender.close();

        verify(c, times(1)).close(timeoutInMs);
    }

    @Test
    public void trackReturnedOptionWillMarkReturnedMessage() throws Exception {
        int messageCount = 10;
        Flux<OutboundMessage> msgFlux = Flux.range(0, messageCount).map(i -> {
            if (i == 3) {
                return new OutboundMessage("", "non-existing-queue", (i + "").getBytes());
            } else {
                return new OutboundMessage("", queue, (i + "").getBytes());
            }
        });

        sender = createSender();
        SendOptions sendOptions = new SendOptions().trackReturned(true);

        CountDownLatch confirmedLatch = new CountDownLatch(messageCount);
        sender.sendWithPublishConfirms(msgFlux, sendOptions).subscribe(outboundMessageResult -> {
            String body = new String(outboundMessageResult.getOutboundMessage().getBody());
            if ("3".equals(body)) {
                assertThat(outboundMessageResult.isReturned()).isTrue();
                assertThat(outboundMessageResult.isAck()).isTrue();
            } else {
                assertThat(outboundMessageResult.isReturned()).isFalse();
                assertThat(outboundMessageResult.isAck()).isTrue();
            }
            confirmedLatch.countDown();
        });
        assertThat(confirmedLatch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void sendingTypedCorrelationMetadataResultsInTypedCorrelationMetadataOnResults() throws Exception {
        int messageCount = 10;
        Flux<CorrelableOutboundMessage<Integer>> msgFlux = Flux.range(0, messageCount)
            .map(i -> new CorrelableOutboundMessage<>("", queue, null, (i + "").getBytes(), i * 2));

        sender = createSender();

        CountDownLatch confirmedLatch = new CountDownLatch(messageCount);
        sender.sendWithTypedPublishConfirms(msgFlux).subscribe(outboundMessageResult -> {
            String body = new String(outboundMessageResult.getOutboundMessage().getBody());
            assertThat(outboundMessageResult.getOutboundMessage().getCorrelationMetadata())
                .isEqualTo(Integer.parseInt(body) * 2);
            confirmedLatch.countDown();
        });

        assertThat(confirmedLatch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void closeIsIdempotent() throws Exception {
        Sender sender = createSender();
        int nbMessages = 10;
        CountDownLatch latch = new CountDownLatch(nbMessages);
        Channel channel = connection.createChannel();
        channel.basicConsume(queue, true, (consumerTag, message) -> latch.countDown(), consumerTag -> {
        });
        sender.send(Flux.range(1, 10).map(i -> new OutboundMessage("", queue, "".getBytes()))).block();
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        sender.close();
        sender.close();
    }

    @Test public void exchangeToExchangeBindingUnbinding() throws Exception {
        Channel channel = connection.createChannel();

        String e1 = "e2e.from";
        String e2 = "e2e.to";
        try {
            channel.exchangeDeclare(e1, BuiltinExchangeType.DIRECT);
            channel.exchangeDeclare(e2, BuiltinExchangeType.FANOUT);

            sender = createSender();
            sender.bindExchange(BindingSpecification.exchangeBinding(e1, "rk", e2))
                    .then(sender.bindQueue(BindingSpecification.queueBinding(e2, "does-not-matter", queue)))
                    .block(Duration.ofSeconds(5));

            String body = UUID.randomUUID().toString();
            AtomicReference<String> bodyCaptor = new AtomicReference<>();
            CountDownLatch receivedLatch = new CountDownLatch(1);
            channel.basicConsume(queue, true, (consumerTag, message) -> {
                bodyCaptor.set(new String(message.getBody()));
                receivedLatch.countDown();
            }, ctag -> {});

            channel.basicPublish(e1, "rk", null, body.getBytes());

            assertThat(receivedLatch.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(bodyCaptor).hasValue(body);

            CountDownLatch returnedLatch = new CountDownLatch(1);
            channel.addReturnListener(returnMessage -> {
                bodyCaptor.set(new String(returnMessage.getBody()));
                returnedLatch.countDown();
            });

            sender.unbindExchange(BindingSpecification.binding(e1, "rk", e2)).block(Duration.ofSeconds(5));

            body = UUID.randomUUID().toString();
            channel.basicPublish(e1, "rk", true, null, body.getBytes());

            assertThat(returnedLatch.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(bodyCaptor).hasValue(body);

        } finally {
            channel.exchangeDelete(e1);
            channel.exchangeDelete(e2);
        }
    }

    @Test
    public void publishConfirmSubscriptionCompletingRemovesAllListeners() throws Exception {
        Sender sender = createSender();
        int nbMessages = 1;
        CountDownLatch latch = new CountDownLatch(nbMessages);
        Channel channel = connection.createChannel();
        channel.basicConsume(queue, true, (consumerTag, message) -> latch.countDown(), consumerTag -> {
        });
        Channel sendChannel = connection.createChannel();
        sender.sendWithPublishConfirms(Flux.range(1, 10).map(i -> new OutboundMessage("", queue, "".getBytes())),
                new SendOptions().channelMono(Mono.just(sendChannel))).blockLast();
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        // there is no API way to access the listeners, we are therefore extracting directly from the field
        assertThat(sendChannel).extracting("shutdownHooks").asList().isEmpty();
        assertThat(sendChannel).extracting("returnListeners").asList().isEmpty();
        assertThat(sendChannel).extracting("confirmListeners").asList().isEmpty();

        sender.close();
    }
}
