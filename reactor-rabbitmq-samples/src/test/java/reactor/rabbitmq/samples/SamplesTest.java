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

package reactor.rabbitmq.samples;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class SamplesTest {

    @Test public void SenderReceiver() throws Exception {
        String queue = "demo-queue";
        int count = 10;
        CountDownLatch sendLatch = new CountDownLatch(count);
        CountDownLatch receiveLatch = new CountDownLatch(count);
        SampleReceiver receiver = new SampleReceiver();
        receiver.consume(queue, receiveLatch);
        SampleSender sender = new SampleSender();
        sender.send(queue, count, sendLatch);
        assertTrue(sendLatch.await(10, TimeUnit.SECONDS));
        assertTrue(receiveLatch.await(10, TimeUnit.SECONDS));
        receiver.close();
        sender.close();
    }

}
