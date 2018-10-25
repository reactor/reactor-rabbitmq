import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.rabbitmq.OutboundMessage
import reactor.rabbitmq.QueueSpecification
@GrabResolver(name = 'spring-staging', root = 'http://repo.spring.io/libs-staging-local/')
@Grab(group = 'io.projectreactor.rabbitmq', module = 'reactor-rabbitmq', version = "${version}")
@Grab(group = 'org.slf4j', module = 'slf4j-simple', version = '1.7.25')

import reactor.rabbitmq.ReactorRabbitMq
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

try {
    def latch = new CountDownLatch(10)
    def sender = ReactorRabbitMq.createSender()
    sender.declare(QueueSpecification.queue("").autoDelete(true))
            .subscribe({ q ->
        ReactorRabbitMq.createReceiver().consumeNoAck(q.getQueue())
                .subscribe({ d -> latch.countDown() })
        def messages = Flux.range(1, 10)
                .map({ i -> new OutboundMessage("", q.getQueue(), "".getBytes()) })
        sender.send(messages).subscribe()
    })
    latch.await(5, TimeUnit.SECONDS)
    def received = latch.await(5, TimeUnit.SECONDS)
    if (!received)
        throw new IllegalStateException("Didn't receive message in 5 seconds")
    LoggerFactory.getLogger("rabbitmq").info("Test succeeded")
    System.exit 0
} catch(Exception e) {
    LoggerFactory.getLogger("rabbitmq").info("Test failed", e)
    System.exit 1
}