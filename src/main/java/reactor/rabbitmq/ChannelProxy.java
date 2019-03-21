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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Command;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ConsumerShutdownSignalCallback;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.ReturnCallback;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link Channel} proxy that re-open the underlying channel if necessary.
 * <p>
 * This class is used only for resource management in {@link Sender}, so
 * only {@link Channel#asyncCompletableRpc(Method)} checks whether the
 * underlying {@link Channel} is closed or not, and re-open it if
 * necessary. All other methods are not supported.
 */
public class ChannelProxy implements Channel {

    private final Connection connection;
    private final Lock delegateLock = new ReentrantLock();
    private volatile Channel delegate;

    public ChannelProxy(Connection connection) throws IOException {
        this.connection = connection;
        this.delegate = connection.createChannel();
    }

    @Override
    public int getChannelNumber() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Connection getConnection() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close(int closeCode, String closeMessage) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort(int closeCode, String closeMessage) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addReturnListener(ReturnListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ReturnListener addReturnListener(ReturnCallback returnCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeReturnListener(ReturnListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearReturnListeners() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addConfirmListener(ConfirmListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConfirmListener addConfirmListener(ConfirmCallback ackCallback, ConfirmCallback nackCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeConfirmListener(ConfirmListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearConfirmListeners() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Consumer getDefaultConsumer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDefaultConsumer(Consumer consumer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void basicQos(int prefetchSize, int prefetchCount, boolean global) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void basicQos(int prefetchCount, boolean global) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void basicQos(int prefetchCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void basicPublish(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void basicPublish(String exchange, String routingKey, boolean mandatory, AMQP.BasicProperties props, byte[] body) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void basicPublish(String exchange, String routingKey, boolean mandatory, boolean immediate, AMQP.BasicProperties props, byte[] body) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete, Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete,
        Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete, boolean internal,
        Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete, boolean internal,
        Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void exchangeDeclareNoWait(String exchange, String type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void exchangeDeclareNoWait(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete, boolean internal,
        Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclarePassive(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.DeleteOk exchangeDelete(String exchange, boolean ifUnused) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void exchangeDeleteNoWait(String exchange, boolean ifUnused) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.DeleteOk exchangeDelete(String exchange) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.BindOk exchangeBind(String destination, String source, String routingKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.BindOk exchangeBind(String destination, String source, String routingKey, Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void exchangeBindNoWait(String destination, String source, String routingKey, Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.UnbindOk exchangeUnbind(String destination, String source, String routingKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.UnbindOk exchangeUnbind(String destination, String source, String routingKey, Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void exchangeUnbindNoWait(String destination, String source, String routingKey, Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Queue.DeclareOk queueDeclare() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Queue.DeclareOk queueDeclare(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void queueDeclareNoWait(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Queue.DeclareOk queueDeclarePassive(String queue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Queue.DeleteOk queueDelete(String queue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Queue.DeleteOk queueDelete(String queue, boolean ifUnused, boolean ifEmpty) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void queueDeleteNoWait(String queue, boolean ifUnused, boolean ifEmpty) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void queueBindNoWait(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Queue.PurgeOk queuePurge(String queue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GetResponse basicGet(String queue, boolean autoAck) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void basicAck(long deliveryTag, boolean multiple) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void basicReject(long deliveryTag, boolean requeue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, Consumer callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback,
        ConsumerShutdownSignalCallback shutdownSignalCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Consumer callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback, CancelCallback cancelCallback,
        ConsumerShutdownSignalCallback shutdownSignalCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, Consumer callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, DeliverCallback deliverCallback,
        ConsumerShutdownSignalCallback shutdownSignalCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, DeliverCallback deliverCallback, CancelCallback cancelCallback,
        ConsumerShutdownSignalCallback shutdownSignalCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, Consumer callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback,
        ConsumerShutdownSignalCallback shutdownSignalCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback, CancelCallback cancelCallback,
        ConsumerShutdownSignalCallback shutdownSignalCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments,
        Consumer callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments,
        DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments,
        DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments,
        DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void basicCancel(String consumerTag) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Basic.RecoverOk basicRecover() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Basic.RecoverOk basicRecover(boolean requeue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Tx.SelectOk txSelect() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Tx.CommitOk txCommit() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Tx.RollbackOk txRollback() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Confirm.SelectOk confirmSelect() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getNextPublishSeqNo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean waitForConfirms() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean waitForConfirms(long timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void waitForConfirmsOrDie() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void waitForConfirmsOrDie(long timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void asyncRpc(Method method) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Command rpc(Method method) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long messageCount(String queue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long consumerCount(String queue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Command> asyncCompletableRpc(Method method) throws IOException {
        if (!delegate.isOpen()) {
            openNewChannelIfNecessary();
        }
        return delegate.asyncCompletableRpc(method);
    }

    private void openNewChannelIfNecessary() throws IOException {
        try {
            delegateLock.lock();
            if (delegate.isOpen()) {
                return;
            }
            this.delegate = connection.createChannel();
        } finally {
            delegateLock.unlock();
        }
    }

    @Override
    public void addShutdownListener(ShutdownListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeShutdownListener(ShutdownListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ShutdownSignalException getCloseReason() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void notifyListeners() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOpen() {
        throw new UnsupportedOperationException();
    }
}
