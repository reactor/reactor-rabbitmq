/*
 * Copyright (c) 2019-2020 VMware, Inc. or its affiliates.
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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class QueueSpecificationTest {

    @Nested
    class NullName {
        @Test
        void nullNameShouldReturnANonDurableQueue() {
            assertFalse(QueueSpecification.queue().isDurable());
        }

        @Test
        void passingNullNameShouldReturnANonDurableQueue() {
            assertFalse(QueueSpecification.queue(null).isDurable());
        }

        @Test
        void nullNameShouldNotAbleToConfigureDurableToTrue() {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> QueueSpecification.queue().durable(true),
                    "Once a queue has a null name, durable is always false");
        }

        @Test
        void nullNameShouldKeepDurableWhenConfigureDurableToFalse() {
            assertFalse(QueueSpecification.queue().durable(false).isDurable());
        }

        @Test
        void passingNullNameShouldReturnAExclusiveQueue() {
            assertTrue(QueueSpecification.queue().isExclusive());
        }

        @Test
        void nullNameShouldNotAbleToConfigureExclusiveToFalse() {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> QueueSpecification.queue().exclusive(false),
                    "Once a queue has a null name, exclusive is always true");
        }

        @Test
        void nullNameShouldKeepDurableWhenConfigureExclusiveToTrue() {
            assertTrue(QueueSpecification.queue().exclusive(true).isExclusive());
        }

        @Test
        void passingNullNameShouldReturnAnAutoDeleteQueue() {
            assertTrue(QueueSpecification.queue().isAutoDelete());
        }

        @Test
        void nullNameShouldNotAbleToConfigureAutoDeleteToFalse() {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> QueueSpecification.queue().autoDelete(false),
                    "Once a queue has a null name, autoDelete is always true");
        }

        @Test
        void nullNameShouldKeepAutoDeleteWhenConfigureAutoDeleteToTrue() {
            assertTrue(QueueSpecification.queue().autoDelete(true).isAutoDelete());
        }

        @Test
        void passingANonNullNameAfterShouldReturnAConfigurableQueueSpecification() {
            QueueSpecification queueSpecification = QueueSpecification.queue()
                    .name("not-null-anymore")
                    .durable(true)
                    .exclusive(false)
                    .autoDelete(false)
                    .passive(false);

            assertEquals(queueSpecification.getName(), "not-null-anymore");
            assertTrue(queueSpecification.isDurable());
            assertFalse(queueSpecification.isAutoDelete());
            assertFalse(queueSpecification.isExclusive());
            assertFalse(queueSpecification.isPassive());
        }
    }

    @Nested
    class NotNullName {
        @Test
        void queueSpecificationShouldReturnCorrespondingProperties() {
            QueueSpecification queueSpecification = QueueSpecification.queue("my-queue")
                    .durable(false)
                    .autoDelete(false)
                    .exclusive(false)
                    .passive(true);

            assertEquals(queueSpecification.getName(), "my-queue");
            assertFalse(queueSpecification.isDurable());
            assertFalse(queueSpecification.isAutoDelete());
            assertFalse(queueSpecification.isExclusive());
            assertTrue(queueSpecification.isPassive());
            assertNull(queueSpecification.getArguments());
        }

        @Test
        void queueSpecificationShouldReturnCorrespondingPropertiesWhenEmptyName() {
            QueueSpecification queueSpecification = QueueSpecification.queue("")
                    .durable(false)
                    .autoDelete(false)
                    .exclusive(false)
                    .passive(false);

            assertEquals(queueSpecification.getName(), "");
            assertFalse(queueSpecification.isDurable());
            assertFalse(queueSpecification.isAutoDelete());
            assertFalse(queueSpecification.isExclusive());
            assertFalse(queueSpecification.isPassive());
            assertNull(queueSpecification.getArguments());
        }

        @Test
        public void queueSpecificationShouldKeepArgumentsWhenNullName() {
            QueueSpecification queueSpecification = QueueSpecification.queue("")
                    .arguments(Collections.singletonMap("x-max-length", 1000))
                    .name(null);
            assertNull(queueSpecification.getName());
            assertFalse(queueSpecification.isDurable());
            assertTrue(queueSpecification.isAutoDelete());
            assertTrue(queueSpecification.isExclusive());
            assertFalse(queueSpecification.isPassive());
            assertThat(queueSpecification.getArguments()).isNotNull().hasSize(1).containsKeys("x-max-length");
        }
    }
}