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

import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.XidImpl;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Transaction to push messages into a queue. It is created by a QueueSender
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 * @see QueueSender#createTransaction
 */
public class QueuePushTransaction extends QueueTransaction {
    AtomicInteger size = new AtomicInteger(0);

    /**
     * Creates a new QueuePushTransaction
     *
     * @param abstractQueue           the queue
     * @param transactionId           the transaction id
     * @param queueTransactionHandler the queue transaction handler
     */
    QueuePushTransaction(AbstractQueue abstractQueue, Object transactionId, QueueTransactionHandler queueTransactionHandler) {
        super(abstractQueue, transactionId, queueTransactionHandler);
    }

    public QueuePushTransaction() {
    }

    void restart(AbstractQueue abstractQueue, Object transactionId, QueueTransactionHandler queueTransactionHandler) {
        super.restart(abstractQueue, transactionId, queueTransactionHandler);
        size.set(0);
    }

    /**
     * Returns the flow control delay, computed after the transaction has been committed.
     *
     * @return flow control delay.
     */
    public long getFlowControlDelay() {
        FlowController fc = abstractQueue.getFlowController();
        return fc == null ? 0 : fc.getNewDelay();
    }

    /**
     * Checks all registered receivers and, if set, their message selectors
     * whether this message will be received by one of them.
     *
     * @param message
     * @return has receiver or not
     */
    public boolean hasReceiver(MessageImpl message) {
        return abstractQueue.hasReceiver(message);
    }

    /**
     * Check whether the queue has enough space to store the number of messages in the transaction
     *
     * @return true/false
     */
    public boolean hasSpaceLeft() {
        int maxMessages = abstractQueue.getMaxMessages();
        boolean rc = false;
        try {
            rc = !(maxMessages > 0 && size.get() + abstractQueue.getNumberQueueMessages() > maxMessages);
        } catch (QueueException e) {
            e.printStackTrace();
        }
        return rc;
    }

    /**
     * Put a message into the queue
     *
     * @param message the message
     * @throws QueueException                  thrown by the queue
     * @throws QueueTransactionClosedException if the transaction is closed
     */
    public void putMessage(MessageImpl message)
            throws QueueException, QueueTransactionClosedException {
        verifyTransactionState();
        abstractQueue.putMessage(transactionId, message);
        size.incrementAndGet();
    }

    /**
     * Asynchronously commits the transaction and set the transaction state to closed
     *
     * @param callback async completion callback
     */
    public void commit(AsyncCompletionCallback callback) {
        try {
            verifyTransactionState();
        } catch (QueueTransactionClosedException e) {
            callback.setException(e);
            callback.done(false);
            return;
        }
        setClosed(true);
        abstractQueue.commit(transactionId, callback);
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
        abstractQueue.rollback(transactionId, globalTransactionId, false);
    }

    /**
     * Rollback of the transaction.
     *
     * @throws QueueException                  thrown by the queue
     * @throws QueueTransactionClosedException if the transaction is closed
     */
    public void rollback()
            throws QueueException, QueueTransactionClosedException {
        verifyTransactionState();
        setClosed(true);
        abstractQueue.rollback(transactionId, false);
    }

    /**
     * Asynchronous rollback of the transaction.
     *
     * @param callback async completion callback
     */
    public void rollback(AsyncCompletionCallback callback) {
        try {
            verifyTransactionState();
        } catch (QueueTransactionClosedException e) {
            callback.setException(e);
            callback.done(false);
            return;
        }
        setClosed(true);
        abstractQueue.rollback(transactionId, false, callback);
    }
}

