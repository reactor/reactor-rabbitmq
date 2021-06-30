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

package reactor.rabbitmq;

import com.rabbitmq.client.BlockedCallback;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.UnblockedCallback;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implement {@link Connection} that ensure closing is idempotent.
 * It is used internally to share the same AMQP connection between
 * {@link Sender} and {@link Receiver} instances, e.g. to use exclusive
 * resources.
 */
public class IdempotentClosedConnection implements Connection {

    private final Connection delegate;

    private final AtomicBoolean closingOrClosed = new AtomicBoolean(false);

    public IdempotentClosedConnection(Connection delegate) {
        this.delegate = delegate;
    }

    public Connection getDelegate() {
        return delegate;
    }

    @Override
    public InetAddress getAddress() {
        return this.delegate.getAddress();
    }

    @Override
    public int getPort() {
        return this.delegate.getPort();
    }

    @Override
    public int getChannelMax() {
        return this.delegate.getChannelMax();
    }

    @Override
    public int getFrameMax() {
        return this.delegate.getFrameMax();
    }

    @Override
    public int getHeartbeat() {
        return this.delegate.getHeartbeat();
    }

    @Override
    public Map<String, Object> getClientProperties() {
        return this.delegate.getClientProperties();
    }

    @Override
    @Nullable
    public String getClientProvidedName() {
        return this.delegate.getClientProvidedName();
    }

    @Override
    public Map<String, Object> getServerProperties() {
        return this.delegate.getServerProperties();
    }

    @Override
    @Nullable
    public Channel createChannel() throws IOException {
        return this.delegate.createChannel();
    }

    @Override
    @Nullable
    public Channel createChannel(int channelNumber) throws IOException {
        return this.delegate.createChannel(channelNumber);
    }

    @Override
    public void close() throws IOException {
        if (shouldCloseOrAbort()) {
            this.delegate.close();
        }
    }

    @Override
    public void close(int closeCode, String closeMessage) throws IOException {
        if (shouldCloseOrAbort()) {
            this.delegate.close(closeCode, closeMessage);
        }
    }

    @Override
    public void close(int timeout) throws IOException {
        if (shouldCloseOrAbort()) {
            this.delegate.close(timeout);
        }
    }

    @Override
    public void close(int closeCode, String closeMessage, int timeout) throws IOException {
        if (shouldCloseOrAbort()) {
            this.delegate.close(closeCode, closeMessage, timeout);
        }
    }

    @Override
    public void abort() {
        if (shouldCloseOrAbort()) {
            this.delegate.abort();
        }
    }

    @Override
    public void abort(int closeCode, String closeMessage) {
        if (shouldCloseOrAbort()) {
            this.delegate.abort(closeCode, closeMessage);
        }
    }

    @Override
    public void abort(int timeout) {
        if (shouldCloseOrAbort()) {
            this.delegate.abort(timeout);
        }
    }

    @Override
    public void abort(int closeCode, String closeMessage, int timeout) {
        if (shouldCloseOrAbort()) {
            this.delegate.abort(closeCode, closeMessage, timeout);
        }
    }

    private boolean shouldCloseOrAbort() {
        return this.closingOrClosed.compareAndSet(false, true);
    }

    @Override
    public void addBlockedListener(BlockedListener listener) {
        this.delegate.addBlockedListener(listener);
    }

    @Override
    public BlockedListener addBlockedListener(BlockedCallback blockedCallback, UnblockedCallback unblockedCallback) {
        return this.delegate.addBlockedListener(blockedCallback, unblockedCallback);
    }

    @Override
    public boolean removeBlockedListener(BlockedListener listener) {
        return this.delegate.removeBlockedListener(listener);
    }

    @Override
    public void clearBlockedListeners() {
        this.delegate.clearBlockedListeners();
    }

    @Override
    public ExceptionHandler getExceptionHandler() {
        return this.delegate.getExceptionHandler();
    }

    @Override
    public String getId() {
        return this.delegate.getId();
    }

    @Override
    public void setId(String id) {
        this.delegate.setId(id);
    }

    @Override
    public void addShutdownListener(ShutdownListener listener) {
        this.delegate.addShutdownListener(listener);
    }

    @Override
    public void removeShutdownListener(ShutdownListener listener) {
        this.delegate.removeShutdownListener(listener);
    }

    @Override
    @Nullable
    public ShutdownSignalException getCloseReason() {
        return this.delegate.getCloseReason();
    }

    @Override
    public void notifyListeners() {
        this.delegate.notifyListeners();
    }

    @Override
    public boolean isOpen() {
        return this.delegate.isOpen();
    }
}
