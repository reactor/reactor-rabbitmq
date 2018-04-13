/*
 * Copyright (c) 2018 Pivotal Software Inc, All Rights Reserved.
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

package reactor.rabbitmq.docs;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.ConsumeOptions;
import reactor.rabbitmq.ExceptionHandlers;
import reactor.rabbitmq.ReactorRabbitMq;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.ReceiverOptions;
import reactor.rabbitmq.SenderOptions;

import java.time.Duration;
import java.util.function.BiConsumer;

/**
 *
 */
@SuppressWarnings("unused")
public class ApiGuideReceiver {

    void optionsSimple() {
        // tag::options-simple[]
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();

        ReceiverOptions receiverOptions =  new ReceiverOptions()
            .connectionFactory(connectionFactory)                       // <1>
            .connectionSubscriptionScheduler(Schedulers.elastic());     // <2>
        // end::options-simple[]
        // tag::inbound-flux[]
        Flux<Delivery> inboundFlux = ReactorRabbitMq.createReceiver(receiverOptions)
                .consumeNoAck("reactive.queue");
        // end::inbound-flux[]

        Receiver receiver = ReactorRabbitMq.createReceiver();
        // tag::closing[]
        receiver.close();
        // end::closing[]
    }

    void optionsConnectionSupplier() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        // tag::options-connection-supplier[]
        SenderOptions senderOptions =  new SenderOptions()
            .connectionFactory(connectionFactory)
            .connectionSupplier(cf -> cf.newConnection(                                  // <1>
                new Address[] {new Address("192.168.0.1"), new Address("192.168.0.2")},
                "reactive-sender"))
            .resourceCreationScheduler(Schedulers.elastic());
        // end::options-connection-supplier[]
    }

    void ackExceptionHandler() {
        // tag::auto-ack-retry-settings[]
        Flux<Delivery> inboundFlux = ReactorRabbitMq
            .createReceiver()
            .consumeNoAck("reactive.queue", new ConsumeOptions()
                .exceptionHandler(new ExceptionHandlers.RetryAcknowledgmentExceptionHandler(
                    Duration.ofSeconds(20), Duration.ofMillis(500), // <1>
                    ExceptionHandlers.CONNECTION_RECOVERY_PREDICATE
                ))
            );
        // end::auto-ack-retry-settings[]
    }

    void manualAckRetry() {
        // tag::manual-ack-retry[]
        Receiver receiver = ReactorRabbitMq.createReceiver();
        BiConsumer<Receiver.AcknowledgmentContext, Exception> exceptionHandler =
            new ExceptionHandlers.RetryAcknowledgmentExceptionHandler(                  // <1>
                Duration.ofSeconds(20), Duration.ofMillis(500),
                ExceptionHandlers.CONNECTION_RECOVERY_PREDICATE
        );
        receiver.consumeManualAck("queue")
            .subscribe(msg -> {
                // ...                                                                   // <2>
                try {
                    msg.ack();                                                           // <3>
                } catch (Exception e) {
                    exceptionHandler.accept(new Receiver.AcknowledgmentContext(msg), e); // <4>
                }
            });
        // end::manual-ack-retry[]
    }

}
