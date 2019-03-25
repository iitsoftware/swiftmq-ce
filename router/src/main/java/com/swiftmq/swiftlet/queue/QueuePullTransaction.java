/*
 * Copyright 2019 IIT Software GmbH
 *
 * IIT Software GmbH licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.swiftmq.swiftlet.queue;

import com.swiftmq.jms.XidImpl;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;

import java.util.List;

/**
 * Transaction to pull messages from a queue. It is created by a QueueReceiver
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 * @see QueueReceiver#createTransaction
 */
public class QueuePullTransaction extends QueueTransaction {
    boolean setRedeliveredOnRollback;
    Selector selector = null;
    int viewId = -1;
    long receiverId = -1;

    /**
     * Creates a new QueuePullTransaction
     *
     * @param abstractQueue            the queue
     * @param transactionId            the transaction id
     * @param queueTransactionHandler  the queue transaction handler
     * @param setRedeliveredOnRollback flag if to set a redelivered state on rollback
     */
    QueuePullTransaction(AbstractQueue abstractQueue, Object transactionId, QueueTransactionHandler queueTransactionHandler, boolean setRedeliveredOnRollback) {
        super(abstractQueue, transactionId, queueTransactionHandler);
        this.setRedeliveredOnRollback = setRedeliveredOnRollback;
    }

    public QueuePullTransaction() {
    }

    void restart(AbstractQueue abstractQueue, Object transactionId, QueueTransactionHandler queueTransactionHandler, boolean setRedeliveredOnRollback) {
        super.restart(abstractQueue, transactionId, queueTransactionHandler);
        this.setRedeliveredOnRollback = setRedeliveredOnRollback;
    }

    void setView(Selector selector, int viewId) {
        this.selector = selector;
        this.viewId = viewId;
    }

    void setReceiverId(long receiverId) {
        this.receiverId = receiverId;
    }

    /**
     * Get a message from the queue.
     *
     * @return The message
     * @throws QueueTransactionClosedException if the transaction was closed
     * @throws QueueException                  thrown by the queue
     */
    public MessageEntry getMessage()
            throws QueueTransactionClosedException, QueueException {
        verifyTransactionState();
        return viewId == -1 ? abstractQueue.getMessage(transactionId) : abstractQueue.getMessage(transactionId, selector, viewId);
    }

    /**
     * Get a message from the queue that matches the selector.
     *
     * @param selector A message selector
     * @return The message
     * @throws QueueTransactionClosedException if the transaction was closed
     * @throws QueueException                  thrown by the queue
     */
    public MessageEntry getMessage(Selector selector)
            throws QueueTransactionClosedException, QueueException {
        verifyTransactionState();
        return viewId == -1 ? abstractQueue.getMessage(transactionId, selector) : abstractQueue.getMessage(transactionId, selector, viewId);
    }

    /**
     * Get a message from the queue but wait only a specific period of time.
     *
     * @param timeout a timeout in ms
     * @return The message
     * @throws QueueTransactionClosedException if the transaction was closed
     * @throws QueueException                  thrown by the queue
     * @throws QueueTimeoutException           if a timeout occurs
     */
    public MessageEntry getMessage(long timeout)
            throws QueueTransactionClosedException, QueueException, QueueTimeoutException {
        verifyTransactionState();
        return viewId == -1 ? abstractQueue.getMessage(transactionId, timeout) : abstractQueue.getMessage(transactionId, selector, viewId, timeout);
    }

    /**
     * Get an expired message from the queue but wait only a specific period of time.
     *
     * @param timeout a timeout in ms
     * @return The message
     * @throws QueueTransactionClosedException if the transaction was closed
     * @throws QueueException                  thrown by the queue
     * @throws QueueTimeoutException           if a timeout occurs
     */
    public MessageEntry getExpiredMessage(long timeout)
            throws QueueTransactionClosedException, QueueException, QueueTimeoutException {
        verifyTransactionState();
        return abstractQueue.getExpiredMessage(transactionId, timeout);
    }

    /**
     * Get a message from the queue that matches the selector but wait only a specific period of time.
     *
     * @param timeout  a timeout in ms
     * @param selector a message selector
     * @return The message
     * @throws QueueTransactionClosedException if the transaction was closed
     * @throws QueueException                  thrown by the queue
     * @throws QueueTimeoutException           if a timeout occurs
     */
    public MessageEntry getMessage(long timeout, Selector selector)
            throws QueueTransactionClosedException, QueueException, QueueTimeoutException {
        verifyTransactionState();
        return viewId == -1 ? abstractQueue.getMessage(transactionId, selector, timeout) : abstractQueue.getMessage(transactionId, selector, viewId, timeout);
    }

    /**
     * Remove messages from a queue
     *
     * @param messageIndexes List of MessageIndexes to remove
     * @throws QueueException on error
     * @Since 9.8.0
     */
    public void removeMessages(List<MessageIndex> messageIndexes)
            throws QueueTransactionClosedException, QueueException {
        verifyTransactionState();
        abstractQueue.removeMessages(transactionId, messageIndexes);
    }

    /**
     * Registers a message processor.
     *
     * @param messageProcessor message processor.
     * @throws QueueTransactionClosedException if the transaction was closed.
     * @throws QueueException                  on error.
     */
    public void registerMessageProcessor(MessageProcessor messageProcessor)
            throws QueueTransactionClosedException, QueueException {
        verifyTransactionState();
        messageProcessor.setTransactionId(transactionId);
        messageProcessor.setViewId(viewId);
        messageProcessor.setReceiverId(receiverId);
        abstractQueue.registerMessageProcessor(messageProcessor);
    }

    /**
     * Unregisters a message processor.
     *
     * @param messageProcessor message processor.
     * @throws QueueTransactionClosedException if the transaction was closed.
     * @throws QueueException                  on error.
     */
    public void unregisterMessageProcessor(MessageProcessor messageProcessor)
            throws QueueTransactionClosedException, QueueException {
        verifyTransactionState();
        abstractQueue.unregisterMessageProcessor(messageProcessor);
    }

    /**
     * Acknowledge a single message of this transaction
     *
     * @param messageIndex The message index
     * @throws QueueException thrown by the queue
     */
    public void acknowledgeMessage(MessageIndex messageIndex)
            throws QueueTransactionClosedException, QueueException {
        verifyTransactionState();
        abstractQueue.acknowledgeMessage(transactionId, messageIndex);
    }

    /**
     * Async acknowledge a single message of this transaction and returns its size as the result of the callback.
     *
     * @param messageIndex The message index
     * @param callback     async completion callback
     * @throws QueueException thrown by the queue
     */
    public void acknowledgeMessage(MessageIndex messageIndex, AsyncCompletionCallback callback)
            throws QueueTransactionClosedException {
        verifyTransactionState();
        abstractQueue.acknowledgeMessage(transactionId, messageIndex, callback);
    }

    /**
     * Async acknowledge a list of messages of this transaction and returns their size as the result of the callback.
     *
     * @param messageIndexList A list of message indexes
     * @param callback         async completion callback
     * @throws QueueException thrown by the queue
     */
    public void acknowledgeMessages(List messageIndexList, AsyncCompletionCallback callback)
            throws QueueTransactionClosedException {
        verifyTransactionState();
        abstractQueue.acknowledgeMessages(transactionId, messageIndexList, callback);
    }

    /**
     * Moves a message with the given message index from the source to this transaction.
     *
     * @param messageIndex      message index.
     * @param sourceTransaction source transaction.
     * @throws QueueTransactionClosedException if the transaction was closed.
     * @throws QueueException                  on error.
     */
    public void moveToTransaction(MessageIndex messageIndex, QueuePullTransaction sourceTransaction)
            throws QueueTransactionClosedException, QueueException {
        verifyTransactionState();
        abstractQueue.moveToTransaction(messageIndex, sourceTransaction.transactionId, transactionId);
    }

    /**
     * Moves a message with the given message index from the source to this transaction and returns its size.
     *
     * @param messageIndex      message index.
     * @param sourceTransaction source transaction.
     * @return size of the corresponding message in bytes
     * @throws QueueTransactionClosedException if the transaction was closed.
     * @throws QueueException                  on error.
     */
    public long moveToTransactionReturnSize(MessageIndex messageIndex, QueuePullTransaction sourceTransaction)
            throws QueueTransactionClosedException, QueueException {
        verifyTransactionState();
        return abstractQueue.moveToTransactionReturnSize(messageIndex, sourceTransaction.transactionId, transactionId);
    }

    /**
     * Moves a message with the given message index to this transaction. The source transaction
     * is determined by the message index.
     *
     * @param messageIndex message index.
     * @throws QueueTransactionClosedException if the transaction was closed.
     * @throws QueueException                  on error.
     */
    public void moveToTransaction(MessageIndex messageIndex)
            throws QueueTransactionClosedException, QueueException {
        verifyTransactionState();
        abstractQueue.moveToTransaction(messageIndex, transactionId);
    }

    /**
     * Moves a message with the given message index to this transaction and returns its size. The source transaction
     * is determined by the message index.
     *
     * @param messageIndex message index.
     * @return size of the corresponding message in bytes
     * @throws QueueTransactionClosedException if the transaction was closed.
     * @throws QueueException                  on error.
     */
    public long moveToTransactionReturnSize(MessageIndex messageIndex)
            throws QueueTransactionClosedException, QueueException {
        verifyTransactionState();
        return abstractQueue.moveToTransactionReturnSize(messageIndex, transactionId);
    }

    /**
     * Rolls back the transaction with the given transaction id, eventually prepared
     * under a global transaction id. If
     * the flag <code>setRedelivered</code> is set then the JMS properties for
     * redelivery and delivery count of messages pulled within this transaction
     * are updated.
     *
     * @param globalTransactionId global transaction id
     * @param setRedelivered      specifies JMS redelivery setting
     * @throws QueueException on error
     */
    public void rollback(XidImpl globalTransactionId, boolean setRedelivered)
            throws QueueException, QueueTransactionClosedException {
        verifyTransactionState();
        setClosed(true);
        abstractQueue.rollback(transactionId, globalTransactionId, setRedeliveredOnRollback);
    }

    /**
     * Rollback of the transaction.
     *
     * @throws QueueException                  thrown by the queue
     * @throws QueueTransactionClosedException if the transaction was closed
     */
    public void rollback()
            throws QueueException, QueueTransactionClosedException {
        verifyTransactionState();
        setClosed(true);
        abstractQueue.rollback(transactionId, setRedeliveredOnRollback);
    }
}

