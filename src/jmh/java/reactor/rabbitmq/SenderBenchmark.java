package reactor.rabbitmq;

import com.rabbitmq.client.Connection;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(2)
public class SenderBenchmark {

    Connection connection;
    Sender sender;
    String queue;
    Flux<OutboundMessage> msgFlux;

    @Param({"1", "10", "100", "1000"})
    public int nbMessages;

    @Setup
    public void setupConnection() throws Exception {
        connection = SenderBenchmarkUtils.newConnection();
    }

    @TearDown
    public void closeConnection() throws Exception {
        connection.close();
    }

    @Setup(Level.Iteration)
    public void setupSender() throws Exception {
        queue = SenderBenchmarkUtils.declareQueue(connection);
        sender = RabbitFlux.createSender();
        msgFlux = SenderBenchmarkUtils.outboundMessageFlux(queue, nbMessages);
    }

    @TearDown(Level.Iteration)
    public void tearDownSender() throws Exception {
        SenderBenchmarkUtils.deleteQueue(connection, queue);
        if (sender != null) {
            sender.close();
        }
    }

    @Benchmark
    public void send(Blackhole blackhole) {
        blackhole.consume(sender.send(msgFlux).block());
    }

    @Benchmark
    public void sendWithPublishConfirms(Blackhole blackhole) {
        blackhole.consume(sender.sendWithPublishConfirms(msgFlux).blockLast());
    }
}
