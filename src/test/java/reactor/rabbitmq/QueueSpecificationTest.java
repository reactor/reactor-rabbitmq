/*
 * Copyright (c) 2018-2019 Pivotal Software Inc, All Rights Reserved.
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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class QueueSpecificationTest {

    @Nested
    class NullName {
        @Test
        void nullNameShouldReturnANonDurableQueue() {
            Assertions.assertFalse(
                QueueSpecification.queue().isDurable());
        }

        @Test
        void passingNullNameShouldReturnANonDurableQueue() {
            Assertions.assertFalse(
                QueueSpecification.queue(null).isDurable());
        }

        @Test
        void nullNameShouldNotAbleToConfigureDurableToTrue() {
            Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> QueueSpecification.queue().durable(true),
                "once a queue has null name, durable is always false");
        }

        @Test
        void nullNameShouldKeepDurableWhenConfigureDurableToFalse() {
            Assertions.assertFalse(
                QueueSpecification.queue().durable(false).isDurable());
        }

        @Test
        void passingNullNameShouldReturnAExclusiveQueue() {
            Assertions.assertTrue(
                QueueSpecification.queue().isExclusive());
        }

        @Test
        void nullNameShouldNotAbleToConfigureExclusiveToFalse() {
            Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> QueueSpecification.queue().exclusive(false),
                "once a queue has null name, exclusive is always true");
        }

        @Test
        void nullNameShouldKeepDurableWhenConfigureExclusiveToTrue() {
            Assertions.assertTrue(
                QueueSpecification.queue().exclusive(true).isExclusive());
        }

        @Test
        void passingNullNameShouldReturnAnAutoDeleteQueue() {
            Assertions.assertTrue(
                QueueSpecification.queue().isAutoDelete());
        }

        @Test
        void nullNameShouldNotAbleToConfigureAutoDeleteToFalse() {
            Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> QueueSpecification.queue().autoDelete(false),
                "once a queue has null name, autoDelete is always true");
        }

        @Test
        void nullNameShouldKeepAutoDeleteWhenConfigureAutoDeleteToTrue() {
            Assertions.assertTrue(
                QueueSpecification.queue().autoDelete(true).isAutoDelete());
        }

        @Test
        void passingANonNullNameAfterShouldReturnAConfigurableQueueSpecification() {
            QueueSpecification queueSpecification = QueueSpecification.queue()
                .name("not-null-anymore")
                .durable(true)
                .exclusive(false)
                .autoDelete(false);

            Assertions.assertEquals(queueSpecification.getName(), "not-null-anymore");
            Assertions.assertTrue(queueSpecification.isDurable());
            Assertions.assertFalse(queueSpecification.isAutoDelete());
            Assertions.assertFalse(queueSpecification.isExclusive());
        }
    }

    @Nested
    class NotNullName {
        @Test
        void queueSpecificationShouldReturnCorrespondingProperties() {
            QueueSpecification queueSpecification = QueueSpecification.queue("my-queue")
                .durable(false)
                .autoDelete(false)
                .exclusive(false);

            Assertions.assertEquals(queueSpecification.getName(), "my-queue");
            Assertions.assertFalse(queueSpecification.isDurable());
            Assertions.assertFalse(queueSpecification.isAutoDelete());
            Assertions.assertFalse(queueSpecification.isExclusive());
            Assertions.assertNull(queueSpecification.getArguments());
        }

        @Test
        void queueSpecificationShouldReturnCorrespondingPropertiesWhenEmptyName() {
            QueueSpecification queueSpecification = QueueSpecification.queue("")
                .durable(false)
                .autoDelete(false)
                .exclusive(false);

            Assertions.assertEquals(queueSpecification.getName(), "");
            Assertions.assertFalse(queueSpecification.isDurable());
            Assertions.assertFalse(queueSpecification.isAutoDelete());
            Assertions.assertFalse(queueSpecification.isExclusive());
            Assertions.assertNull(queueSpecification.getArguments());
        }
    }
}