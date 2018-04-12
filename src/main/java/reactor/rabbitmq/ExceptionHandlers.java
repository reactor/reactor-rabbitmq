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

package reactor.rabbitmq;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/**
 *
 */
public class ExceptionHandlers {

    public static class SimpleRetryTemplate {

        private final long timeout;
        private final long waitingTime;
        private final Map<Class<? extends Throwable>, Boolean> retryableExceptions;

        public SimpleRetryTemplate(long timeout, long waitingTime,
            Map<Class<? extends Throwable>, Boolean> retryableExceptions) {
            if (timeout <= 0) {
                throw new IllegalArgumentException("Timeout must be greater than 0");
            }
            if (waitingTime <= 0) {
                throw new IllegalArgumentException("Waiting time must be greater than 0");
            }
            if (timeout <= waitingTime) {
                throw new IllegalArgumentException("Timeout must be greater than waiting time");
            }
            this.timeout = timeout;
            this.waitingTime = waitingTime;
            this.retryableExceptions = retryableExceptions;
        }

        public void retry(Callable<Void> operation, Exception e) {
            if (shouldRetry(e)) {
                int elapsedTime = 0;
                while (elapsedTime < timeout) {
                    try {
                        Thread.sleep(waitingTime);
                    } catch (InterruptedException ie) {
                        throw new ReactorRabbitMqException("Thread interrupted while retry on sending", e);
                    }
                    elapsedTime += waitingTime;
                    try {
                        operation.call();
                        break;
                    } catch (Throwable sendingException) {
                        if (!shouldRetry(sendingException)) {
                            throw new ReactorRabbitMqException("Not retryable exception thrown during retry", sendingException);
                        }
                    }
                }
            } else {
                throw new ReactorRabbitMqException("Not retryable exception, cannot retry", e);
            }
        }

        protected boolean shouldRetry(Throwable throwable) {
            for (Map.Entry<Class<? extends Throwable>, Boolean> retryableException : retryableExceptions.entrySet()) {
                if (retryableException.getKey().isAssignableFrom(throwable.getClass()) &&
                    retryableException.getValue().booleanValue()) {
                    return true;
                }
            }
            return false;
        }

    }

    public static class RetryAcknowledgmentExceptionHandler implements BiConsumer<Receiver.AcknowledgmentContext, Exception> {

        private final SimpleRetryTemplate retryTemplate;

        public RetryAcknowledgmentExceptionHandler(long timeout, long waitingTime,
            Map<Class<? extends Throwable>, Boolean> retryableExceptions) {
            this.retryTemplate = new SimpleRetryTemplate(
                timeout, waitingTime, retryableExceptions
            );
        }

        @Override
        public void accept(Receiver.AcknowledgmentContext acknowledgmentContext, Exception e) {
            retryTemplate.retry(() -> {
                acknowledgmentContext.getDelivery().ack();
                return null;
            }, e);
        }
    }

    public static class RetrySendingExceptionHandler implements BiConsumer<Sender.SendContext, Exception> {

        private final SimpleRetryTemplate retryTemplate;

        public RetrySendingExceptionHandler(long timeout, long waitingTime,
            Map<Class<? extends Throwable>, Boolean> retryableExceptions) {
            this.retryTemplate = new SimpleRetryTemplate(
              timeout, waitingTime, retryableExceptions
            );
        }

        @Override
        public void accept(Sender.SendContext sendContext, Exception e) {
            retryTemplate.retry(() -> {
                sendContext.publish();
                return null;
            }, e);
        }

    }
}
