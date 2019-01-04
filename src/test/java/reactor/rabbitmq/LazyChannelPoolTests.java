package reactor.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;

import static java.time.Duration.ofSeconds;
import static org.mockito.Mockito.*;

class LazyChannelPoolTests {

    LazyChannelPool lazyChannelPool;

    Connection connection;
    Channel channel1, channel2, channel3;

    @BeforeEach
    void setUp() throws IOException {
        connection = mock(Connection.class);
        when(connection.isOpen()).thenReturn(true);
        channel1 = channel(1);
        channel2 = channel(2);
        channel3 = channel(3);
        when(connection.createChannel()).thenReturn(channel1, channel2, channel3);
    }

    @Test
    void testChannelPoolLazyInitialization() throws Exception {
        int maxChannelPoolSize = 2;
        ChannelPoolOptions channelPoolOptions = new ChannelPoolOptions().maxSize(maxChannelPoolSize);
        lazyChannelPool = new LazyChannelPool(Mono.just(connection), channelPoolOptions);

        StepVerifier.withVirtualTime(() ->
                Mono.when(
                        // 1#
                        useChannelFromStartUntil(ofSeconds(1)),
                        // 2#
                        useChannelBetween(ofSeconds(2), ofSeconds(3))
                ))
                .expectSubscription()
                .thenAwait(ofSeconds(3))
                .verifyComplete();

        // Expectations, numbers on the left mean elapsed time in seconds
        // 0 -> 1# creates channel1
        // 1 -> 1# releases channel1, 1# adds channel1 to pool
        // 2 -> 2# obtains channel1 from pool
        // 3 -> 2# releases channel1, 2# adds channel1 to pool

        verifyBasicPublish(channel1, 2);
        verifyBasicPublishNever(channel2);
        verify(channel1, never()).close();

        lazyChannelPool.close();

        verify(channel1).close();
        verify(channel2, never()).close();
    }

    @Test
    void testChannelPoolExceedsMaxPoolSize() throws Exception {
        int maxChannelPoolSize = 2;
        ChannelPoolOptions channelPoolOptions = new ChannelPoolOptions().maxSize(maxChannelPoolSize);
        lazyChannelPool = new LazyChannelPool(Mono.just(connection), channelPoolOptions);

        StepVerifier.withVirtualTime(() ->
                Mono.when(
                        // 1#
                        useChannelBetween(ofSeconds(1), ofSeconds(4)),
                        // 2#
                        useChannelBetween(ofSeconds(2), ofSeconds(5)),
                        // 3#
                        useChannelBetween(ofSeconds(3), ofSeconds(6))
                ))
                .expectSubscription()
                .thenAwait(ofSeconds(6))
                .verifyComplete();

        // Expectations, numbers on the left mean elapsed time in seconds
        // 1 -> 1# creates channel1
        // 2 -> 2# creates channel2
        // 3 -> 3# creates channel3
        // 4 -> 1# releases channel1, 1# adds channel1 to pool
        // 5 -> 2# releases channel2, 2# adds channel2 to pool
        // 6 -> 3# releases channel3, 3# closes channel3 (pool is full)

        verifyBasicPublishOnce(channel1);
        verifyBasicPublishOnce(channel2);
        verifyBasicPublishOnce(channel3);
        verify(channel1, never()).close();
        verify(channel2, never()).close();
        verify(channel3).close();

        lazyChannelPool.close();

        verify(channel1).close();
        verify(channel2).close();
    }

    @Test
    void testChannelPool() throws Exception {
        int maxChannelPoolSize = 1;
        ChannelPoolOptions channelPoolOptions = new ChannelPoolOptions().maxSize(maxChannelPoolSize);
        lazyChannelPool = new LazyChannelPool(Mono.just(connection), channelPoolOptions);

        StepVerifier.withVirtualTime(() ->
                Mono.when(
                        // 1#
                        useChannelFromStartUntil(ofSeconds(3)),
                        // 2#
                        useChannelBetween(ofSeconds(1), ofSeconds(2)),
                        // 3#
                        useChannelBetween(ofSeconds(4), ofSeconds(5))
                ))
                .expectSubscription()
                .thenAwait(ofSeconds(5))
                .verifyComplete();
        // Expectations, numbers on the left mean elapsed time in seconds
        // 0 -> 1# creates channel1
        // 1 -> 2# creates channel2
        // 2 -> 2# releases channel2, 2# adds channel2 to pool
        // 3 -> 1# releases channel1, 1# closes channel1 (pool is full)
        // 4 -> 3# obtains channel2 from pool
        // 5 -> 3# releases channel2, 2# adds channel2 to pool

        verifyBasicPublishOnce(channel1);
        verifyBasicPublish(channel2, 2);
        verify(channel1).close();
        verify(channel2, never()).close();

        lazyChannelPool.close();
        verify(channel2).close();
    }

    private Mono<Void> useChannelFromStartUntil(Duration until) {
        return useChannelBetween(Duration.ZERO, until);
    }

    private Mono<Void> useChannelBetween(Duration from, Duration to) {
        return Mono.delay(from)
                .then(lazyChannelPool.getChannelMono())
                .flatMap(channel ->
                        Mono.just(1)
                                .doOnNext(i -> {
                                    try {
                                        channel.basicPublish("", "", null, "".getBytes());
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                })
                                .delayElement(to.minus(from))
                                .doFinally(signalType -> lazyChannelPool.getChannelCloseHandler().accept(signalType, channel))
                )
                .then();
    }

    private void verifyBasicPublishNever(Channel channel) throws Exception {
        verifyBasicPublish(channel, 0);
    }

    private void verifyBasicPublishOnce(Channel channel) throws Exception {
        verifyBasicPublish(channel, 1);
    }

    private void verifyBasicPublish(Channel channel, int times) throws Exception {
        verify(channel, times(times)).basicPublish(anyString(), anyString(), any(AMQP.BasicProperties.class), any(byte[].class));
    }

    private Channel channel(int channelNumber) {
        Channel channel = mock(Channel.class);
        when(channel.getChannelNumber()).thenReturn(channelNumber);
        when(channel.isOpen()).thenReturn(true);
        when(channel.getConnection()).thenReturn(connection);
        return channel;
    }
}