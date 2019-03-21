/*
 * Copyright (c) 2018 Pivotal Software Inc, All Rights Reserved.
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

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.AuthenticationFailureException;
import com.rabbitmq.client.ShutdownSignalException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static java.time.Duration.ofMillis;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class ExceptionHandlersTests {

    ExceptionHandlers.RetrySendingExceptionHandler exceptionHandler;

    ExceptionHandlers.SimpleRetryTemplate retryTemplate;

    Predicate<Throwable> predicate;

    @Test
    public void retryableExceptions() {
        predicate = new ExceptionHandlers.ExceptionPredicate(singletonMap(Exception.class, true));
        assertTrue(predicate.test(new AlreadyClosedException(new ShutdownSignalException(true, true, null, null))));
        assertFalse(predicate.test(new Throwable()));

        predicate = new ExceptionHandlers.ExceptionPredicate(new HashMap<Class<? extends Throwable>, Boolean>() {{
            put(ShutdownSignalException.class, true);
            put(IOException.class, false);
            put(AuthenticationFailureException.class, true);
        }});

        assertTrue(predicate.test(new ShutdownSignalException(true, true, null, null)),
            "directly retryable");
        assertTrue(predicate.test(new AlreadyClosedException(new ShutdownSignalException(true, true, null, null))),
            "retryable from its super-class");
        assertFalse(predicate.test(new IOException()), "not retryable");
        assertTrue(predicate.test(new AuthenticationFailureException("")), "directly retryable");
    }

    @Test
    public void shouldThrowWrapperExceptionIfNotImmediatlyRetryable() {
        retryTemplate = retryTemplate(singletonMap(IOException.class, true));
        assertThrows(RabbitFluxException.class, () -> retryTemplate.retry(() -> null, new IllegalArgumentException()));
    }

    @Test
    public void connectionRecoveryTriggering() {
        predicate = new ExceptionHandlers.ConnectionRecoveryTriggeringPredicate();
        assertTrue(predicate.test(new ShutdownSignalException(true, false, null, null)), "hard error, not triggered by application");
        assertTrue(predicate.test(new ShutdownSignalException(false, false, null, null)), "soft error, not triggered");
        assertFalse(predicate.test(new ShutdownSignalException(false, true, null, null)), "soft error, triggered by application");
    }

    @Test
    void retryTimeoutIsReached() {
        exceptionHandler = new ExceptionHandlers.RetrySendingExceptionHandler(
            ofMillis(100), ofMillis(10), new ExceptionHandlers.ExceptionPredicate(singletonMap(Exception.class, true))
        );
        exceptionHandler.accept(sendContext(() -> {
            throw new Exception();
        }), new Exception());
    }

    @Test
    void retrySucceeds() {
        exceptionHandler = new ExceptionHandlers.RetrySendingExceptionHandler(
            ofMillis(100), ofMillis(10), new ExceptionHandlers.ExceptionPredicate(singletonMap(Exception.class, true))
        );
        AtomicLong counter = new AtomicLong(0);
        exceptionHandler.accept(sendContext(() -> {
            if (counter.incrementAndGet() < 3) {
                throw new Exception();
            }
            return null;
        }), new Exception());
        assertEquals(3, counter.get());
    }

    private ExceptionHandlers.SimpleRetryTemplate retryTemplate(Map<Class<? extends Throwable>, Boolean> retryableExceptions) {
        return new ExceptionHandlers.SimpleRetryTemplate(
            ofMillis(100), ofMillis(10), new ExceptionHandlers.ExceptionPredicate(retryableExceptions)
        );
    }

    private Sender.SendContext sendContext(Callable<Void> callable) {
        return new Sender.SendContext(null, null) {

            @Override
            public void publish() throws Exception {
                callable.call();
            }
        };
    }
}
