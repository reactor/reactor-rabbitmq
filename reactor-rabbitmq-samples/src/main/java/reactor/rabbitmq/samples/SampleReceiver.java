/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.rabbitmq.samples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.rabbitmq.QueueSpecification;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.Sender;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class SampleReceiver {

    private static final String QUEUE = "demo-queue";
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleReceiver.class);

    private final Receiver receiver;
    private final Sender sender;

    public SampleReceiver() {
        this.receiver = RabbitFlux.createReceiver();
        this.sender = RabbitFlux.createSender();
    }

    public Disposable consume(String queue, CountDownLatch latch) {
        return receiver.consumeAutoAck(queue)
                       .delaySubscription(sender.declareQueue(QueueSpecification.queue(queue)))
                       .subscribe(m -> {
                           LOGGER.info("Received message {}", new String(m.getBody()));
                           latch.countDown();
                       });
    }

    public void close() {
        this.sender.close();
        this.receiver.close();
    }

    public static void main(String[] args) throws Exception {
        int count = 20;
        CountDownLatch latch = new CountDownLatch(count);
        SampleReceiver receiver = new SampleReceiver();
        Disposable disposable = receiver.consume(QUEUE, latch);
        latch.await(10, TimeUnit.SECONDS);
        disposable.dispose();
        receiver.close();
    }

}
