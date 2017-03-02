package reactor.rabbitmq;

import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 *
 */
public class EmitterCancellationTest {

    @Test
    public void emitterCancellation() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        AtomicInteger cancelCount = new AtomicInteger();
        AtomicInteger count = new AtomicInteger();
        for(int i = 0; i < 100; i++) {
            DummyConsumer consumer = new DummyConsumer();
            Flux<Integer> flux = Flux.create(unsafeEmitter -> {
                FluxSink<Integer> emitter = unsafeEmitter.serialize();
                consumer.emitter = emitter;
                emitter.setCancellation(() -> cancelCount.incrementAndGet());
            }, FluxSink.OverflowStrategy.BUFFER);

            CountDownLatch latch = new CountDownLatch(100);
            Disposable subscription = flux.subscribe(value -> {
                count.incrementAndGet();
                latch.countDown();
            });

            for(int j = 0; j < 100; j++) {
                 executorService.submit(() -> consumer.accept(1));
            }

            latch.await(1, TimeUnit.SECONDS);

            subscription.dispose();
        }
        System.out.println(cancelCount);
        System.out.println(count);
    }

    static class DummyConsumer implements Consumer<Integer> {

        volatile FluxSink<Integer> emitter;

        @Override
        public void accept(Integer integer) {
            emitter.next(integer);
//            System.out.println(Thread.currentThread());
        }
    }

}
