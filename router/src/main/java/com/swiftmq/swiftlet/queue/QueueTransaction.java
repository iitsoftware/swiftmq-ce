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
import com.swiftmq.swiftlet.store.CompositeStoreTransaction;
import com.swiftmq.tools.gc.Recyclable;

/**
 * Abstract base class for queue transactions.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public abstract class QueueTransaction implements Recyclable {
    AbstractQueue abstractQueue;
    boolean closed = false;
    boolean prepared = false;
    Object transactionId;
    QueueTransactionHandler queueTransactionHandler;
    int recycleIndex = -1;
    boolean doRollbackOnClose = true;

    /**
     * Creates a new QueueTransaction
     *
     * @param abstractQueue           the queue
     * @param transactionId           the transaction id
     * @param queueTransactionHandler the associated queue transaction handler
     * @SBGen Constructor assigns activeQueue, transactionId, queueTransactionHandler
     */
    QueueTransaction(AbstractQueue abstractQueue, Object transactionId, QueueTransactionHandler queueTransactionHandler) {
        // SBgen: Assign variables
        this.abstractQueue = abstractQueue;
        this.transactionId = transactionId;
        this.queueTransactionHandler = queueTransactionHandler;
        // SBgen: End assign
        setClosed(false);
    }

    public QueueTransaction() {
    }

    void restart(AbstractQueue abstractQueue, Object transactionId, QueueTransactionHandler queueTransactionHandler) {
        this.abstractQueue = abstractQueue;
        this.transactionId = transactionId;
        this.queueTransactionHandler = queueTransactionHandler;
        doRollbackOnClose = true;
        // SBgen: End assign
        setClosed(false);
    }

    public AbstractQueue getAbstractQueue() {
        return abstractQueue;
    }

    public boolean isDoRollbackOnClose() {
        return doRollbackOnClose;
    }

    public void setDoRollbackOnClose(boolean doRollbackOnClose) {
        this.doRollbackOnClose = doRollbackOnClose;
    }

    public void setRecycleIndex(int recycleIndex) {
        this.recycleIndex = recycleIndex;
    }

    public int getRecycleIndex() {
        return recycleIndex;
    }

    public void reset() {
    }

    /**
     * Returns the queue name of the underlying queue
     *
     * @return queue name
     */
    public String getQueueName() {
        return abstractQueue.getQueueName();
    }

    /**
     * Returns the queue
     *
     * @return the queue
     */
    protected AbstractQueue getQueue() {
        return abstractQueue;
    }

    public boolean isQueueRunning() {
        return abstractQueue.isRunning();
    }

    public boolean isTemporaryQueue() {
        return abstractQueue.isTemporary();
    }

    /**
     * Prepares the transaction with the given transaction id. Messages are
     * stored in the queue (on disk when persistent) but not unlocked. The preparation
     * is logged under the global transaction id.
     *
     * @param globalTransactionId global transaction id
     * @throws QueueException on error
     */
    public void prepare(XidImpl globalTransactionId)
            throws QueueException, QueueTransactionClosedException {
        verifyTransactionState();
        abstractQueue.prepare(transactionId, globalTransactionId);
        prepared = true;
    }

    /**
     * Commits a prepared transaction and set the transaction state to closed.
     *
     * @param globalTransactionId global transaction id
     * @throws QueueException on error
     */
    public void commit(XidImpl globalTransactionId)
            throws QueueException, QueueTransactionClosedException {
        verifyTransactionState();
        setClosed(true);
        abstractQueue.commit(transactionId, globalTransactionId);
        prepared = false;
    }

    /**
     * Commits the transaction and set the transaction state to closed
     *
     * @throws QueueException                  thrown by the queue
     * @throws QueueTransactionClosedException if the transaction was already closed
     */
    public void commit()
            throws QueueException, QueueTransactionClosedException {
        verifyTransactionState();
        setClosed(true);
        abstractQueue.commit(transactionId);
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
    public abstract void rollback(XidImpl globalTransactionId, boolean setRedelivered)
            throws QueueException, QueueTransactionClosedException;

    /**
     * Rolls back the transaction. The transaction state should be set to closed.
     *
     * @throws QueueException                  thrown by the queue
     * @throws QueueTransactionClosedException if the transaction was already closed
     */
    public abstract void rollback()
            throws QueueException, QueueTransactionClosedException;

    /**
     * Set the transaction state
     *
     * @param closed true or false
     */
    void setClosed(boolean closed) {
        // SBgen: Assign variable
        this.closed = closed;
        if (closed)
            queueTransactionHandler.recycler.checkIn(this);
    }

    /**
     * Returns the transaction state
     *
     * @return true or false
     */
    public boolean isClosed() {
        // SBgen: Get variable
        return (closed);
    }

    /**
     * Returns the prepared state
     *
     * @return true or false
     */
    public boolean isPrepared() {
        return (prepared);
    }

    /**
     * Verifies the transaction state. If it is closed then a QueueTransactionClosedException
     * will be thrown
     *
     * @throws QueueTransactionClosedException if the transaction is closed
     */
    protected void verifyTransactionState()
            throws QueueTransactionClosedException {
        if (isClosed())
            throw new QueueTransactionClosedException("transaction is closed!");
    }

    /**
     * Locks this queue and holds it until unlockQueue is called.
     */
    public void lockQueue() {
        abstractQueue.lockQueue(transactionId);
    }

    /**
     * Unlocks this queue.
     *
     * @param markAsyncActive blocks queue as async is active
     */
    public void unlockQueue(boolean markAsyncActive) {
        abstractQueue.unlockQueue(transactionId, markAsyncActive);
    }

    /**
     * Unblocks this queue from async active
     */
    public void unmarkAsyncActive() {
        abstractQueue.unmarkAsyncActive(transactionId);
    }

    /**
     * Sets a CompositeStoreTransaction to be used as the current transaction.
     * At the same time it disables prepare/commit/rollback calls inside this queue
     * so that multiple queues can use the same transaction and prepare/commit/rollback
     * is done outside.
     *
     * @param ct composite transaction
     */
    public void setCompositeStoreTransaction(CompositeStoreTransaction ct) {
        abstractQueue.setCompositeStoreTransaction(transactionId, ct);
    }

    /**
     * Returns the current composite store transaction
     *
     * @return composite transaction
     */
    public CompositeStoreTransaction getCompositeStoreTransaction() {
        return abstractQueue.getCompositeStoreTransaction(transactionId);
    }
}

