/*
 * Copyright (c) 2018-2019 Pivotal Software Inc, All Rights Reserved.
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

import com.rabbitmq.client.MissedHeartbeatException;
import com.rabbitmq.client.ShutdownSignalException;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 *
 */
public class ExceptionHandlers {

    public static Predicate<Throwable> CONNECTION_RECOVERY_PREDICATE = new ConnectionRecoveryTriggeringPredicate();

    public static class ConnectionRecoveryTriggeringPredicate implements Predicate<Throwable> {

        @Override
        public boolean test(Throwable throwable) {
            if (throwable instanceof ShutdownSignalException) {
                ShutdownSignalException sse = (ShutdownSignalException) throwable;
                return !sse.isInitiatedByApplication() || (sse.getCause() instanceof MissedHeartbeatException);
            }
            return false;
        }
    }

    public static class ExceptionPredicate implements Predicate<Throwable> {

        private final Map<Class<? extends Throwable>, Boolean> retryableExceptions;

        public ExceptionPredicate(Map<Class<? extends Throwable>, Boolean> retryableExceptions) {
            this.retryableExceptions = retryableExceptions;
        }

        @Override
        public boolean test(Throwable throwable) {
            for (Map.Entry<Class<? extends Throwable>, Boolean> retryableException : retryableExceptions.entrySet()) {
                if (retryableException.getKey().isAssignableFrom(throwable.getClass()) &&
                    retryableException.getValue().booleanValue()) {
                    return true;
                }
            }
            return false;
        }
    }

    public static class SimpleRetryTemplate {

        private final long timeout;
        private final long waitingTime;

        private final Predicate<Throwable> predicate;

        public SimpleRetryTemplate(Duration timeout, Duration waitingTime, Predicate<Throwable> predicate) {
            if (timeout == null || timeout.isNegative() || timeout.isZero()) {
                throw new IllegalArgumentException("Timeout must be greater than 0");
            }
            if (waitingTime == null || timeout.isNegative() || timeout.isNegative()) {
                throw new IllegalArgumentException("Waiting time must be greater than 0");
            }
            if (timeout.compareTo(waitingTime) <= 0) {
                throw new IllegalArgumentException("Timeout must be greater than waiting time");
            }
            if (predicate == null) {
                throw new NullPointerException("Predicate cannot be null");
            }
            this.timeout = timeout.toMillis();
            this.waitingTime = waitingTime.toMillis();
            this.predicate = predicate;
        }

        public void retry(Callable<Void> operation, Exception e) {
            if (predicate.test(e)) {
                int elapsedTime = 0;
                while (elapsedTime < timeout) {
                    try {
                        Thread.sleep(waitingTime);
                    } catch (InterruptedException ie) {
                        throw new RabbitFluxException("Thread interrupted while retry on sending", e);
                    }
                    elapsedTime += waitingTime;
                    try {
                        operation.call();
                        break;
                    } catch (Throwable sendingException) {
                        if (!predicate.test(sendingException)) {
                            throw new RabbitFluxException("Not retryable exception thrown during retry", sendingException);
                        }
                    }
                }
            } else {
                throw new RabbitFluxException("Not retryable exception, cannot retry", e);
            }
        }
    }

    public static class RetryAcknowledgmentExceptionHandler implements BiConsumer<Receiver.AcknowledgmentContext, Exception> {

        private final SimpleRetryTemplate retryTemplate;

        public RetryAcknowledgmentExceptionHandler(Duration timeout, Duration waitingTime,
            Predicate<Throwable> predicate) {
            this.retryTemplate = new SimpleRetryTemplate(
                timeout, waitingTime, predicate
            );
        }

        @Override
        public void accept(Receiver.AcknowledgmentContext acknowledgmentContext, Exception e) {
            retryTemplate.retry(() -> {
                acknowledgmentContext.ackOrNack();
                return null;
            }, e);
        }
    }

    public static class RetrySendingExceptionHandler implements BiConsumer<Sender.SendContext, Exception> {

        private final SimpleRetryTemplate retryTemplate;

        public RetrySendingExceptionHandler(Duration timeout, Duration waitingTime, Predicate<Throwable> predicate) {
            this.retryTemplate = new SimpleRetryTemplate(
                timeout, waitingTime, predicate
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
