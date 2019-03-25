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
import com.swiftmq.swiftlet.queue.event.QueueReceiverListener;
import com.swiftmq.swiftlet.store.CompositeStoreTransaction;
import com.swiftmq.swiftlet.store.PrepareLogRecord;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;

import java.util.List;
import java.util.SortedSet;

/**
 * Abstract base class for queues used in SwiftMQ.
 * All operations on the queue will be controlled by the queue manager. Access to queues
 * are exclusively performed by the resp. accessor classes like QueuePull/PushTransaction.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public abstract class AbstractQueue {
    public static final int SHARED = 0;
    public static final int EXCLUSIVE = 1;
    public static final int ACTIVESTANDBY = 2;

    public static final int AS_MESSAGE = 0;
    public static final int PERSISTENT = 1;
    public static final int NON_PERSISTENT = 2;

    String queueName;
    String localName;
    protected volatile int maxMessages = -1;
    protected volatile int persistenceMode = AS_MESSAGE;
    protected boolean temporary = false;
    protected FlowController flowController = null;
    protected int receiverCount = 0;
    protected long cleanUpInterval = 0;
    QueueReceiverListener queueReceiverListener = null;
    protected int consumerMode = SHARED;

    /**
     * Set the queue name. Will be called from queue manager
     *
     * @param queueName queue name
     */
    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    /**
     * Returns the queue name
     *
     * @return queue name
     */
    public String getQueueName() {
        // SBgen: Get variable
        return (queueName);
    }

    /**
     * Sets the local name.
     *
     * @param localName local name.
     */
    public void setLocalName(String localName) {
        this.localName = localName;
    }

    /**
     * Returns the local name.
     *
     * @return local name.
     */
    public String getLocalName() {
        return localName;
    }

    /**
     * Returns the max. cache size in number messages.
     *
     * @return cache size.
     */
    public int getCacheSize() {
        return 0;
    }

    /**
     * Returns the max cache size in KB
     *
     * @return cache size.
     */
    public int getCacheSizeKB() {
        return 0;
    }

    /**
     * Returns the current cache size in number messages.
     *
     * @return cache size.
     */
    public int getCurrentCacheSizeMessages() {
        return 0;
    }

    /**
     * Returns the current cache size in KB
     *
     * @return cache size.
     */
    public int getCurrentCacheSizeKB() {
        return 0;
    }

    public boolean isAccounting() {
        return false;
    }

    public int getConsumerMode() {
        return consumerMode;
    }

    public void setConsumerMode(int consumerMode) {
        this.consumerMode = consumerMode;
    }

    public void startAccounting(Object accountingProfile) {
    }

    public void flushAccounting() {
    }

    public void stopAccounting() {
    }

    public void addWireTapSubscriber(String name, WireTapSubscriber subscriber) {
    }

    public void removeWireTapSubscriber(String name, WireTapSubscriber subscriber) {

    }

    public synchronized QueueReceiverListener getQueueReceiverListener() {
        return queueReceiverListener;
    }

    public synchronized void setQueueReceiverListener(QueueReceiverListener queueReceiverListener) {
        this.queueReceiverListener = queueReceiverListener;
    }

    /**
     * Sets the flow controller.
     *
     * @param flowController flow controller.
     */
    public synchronized void setFlowController(FlowController flowController) {
        this.flowController = flowController;
        if (flowController != null)
            flowController.setReceiverCount(receiverCount);
    }

    /**
     * Returns the flow controller.
     *
     * @return flow controller.
     */
    public synchronized FlowController getFlowController() {
        return flowController;
    }

    /**
     * Increments the queue receiver count.
     */
    public synchronized void incReceiverCount() {
        receiverCount++;
        if (flowController != null)
            flowController.setReceiverCount(receiverCount);
        if (queueReceiverListener != null)
            queueReceiverListener.receiverCountChanged(this, receiverCount);
    }

    /**
     * Decrements the queue receiver count.
     */
    public synchronized void decReceiverCount() {
        receiverCount--;
        if (flowController != null)
            flowController.setReceiverCount(receiverCount);
        if (queueReceiverListener != null)
            queueReceiverListener.receiverCountChanged(this, receiverCount);
    }

    /**
     * Returns the queue receiver count.
     *
     * @return queue receiver count
     */
    public synchronized int getReceiverCount() {
        return receiverCount;
    }

    /**
     * Internal use
     *
     * @param receiverId
     */
    public void receiverClosed(long receiverId) {
    }

    /**
     * Sets the max. messages.
     *
     * @param maxMessages max messages.
     */
    public void setMaxMessages(int maxMessages) {
        this.maxMessages = maxMessages;
    }

    /**
     * Returns the max. messages.
     *
     * @return max messages.
     */
    public int getMaxMessages() {
        return maxMessages;
    }

    /**
     * Sets the temporary flag.
     *
     * @param b temporary flag.
     */
    public void setTemporary(boolean b) {
        this.temporary = b;
    }

    /**
     * Returns the temporary flag.
     *
     * @return temporary flag.
     */
    public boolean isTemporary() {
        return temporary;
    }

    /**
     * Sets the queue persistence mode.
     *
     * @param persistenceMode persistence mode.
     */
    public void setPersistenceMode(int persistenceMode) {
        this.persistenceMode = persistenceMode;
    }

    /**
     * Returns the queue persistence mode.
     *
     * @return queue persistence mode.
     */
    public int getPersistenceMode() {
        return persistenceMode;
    }

    /**
     * Starts the queue.
     *
     * @throws QueueException on error
     */
    public void startQueue()
            throws QueueException {
    }

    /**
     * Stops the queue.
     *
     * @throws QueueException on error
     */
    public void stopQueue()
            throws QueueException {
    }

    /**
     * Returns if the queue is running or not
     *
     * @return true/false
     */
    public boolean isRunning() {
        return (true); // NYI
    }

    /**
     * Builds a new QueueTransaction on base of the PrepareLogRecord.
     *
     * @return transaction id
     * @throws QueueException on error
     */
    public Object buildPreparedTransaction(PrepareLogRecord logRecord)
            throws QueueException {
        return null;
    }

    /**
     * Creates a new view, based on a selector
     *
     * @return view id
     */
    public int createView(Selector selector) {
        return -1;
    }

    /**
     * Creates a new view, based on a selector
     */
    public void deleteView(int viewId) {
    }

    /**
     * Creates a new push transaction and returns a unique transaction id
     *
     * @return transaction id
     * @throws QueueException on error
     */
    public abstract Object createPushTransaction()
            throws QueueException;

    /**
     * Creates a new pull transaction and returns a unique transaction id
     *
     * @return transaction id
     * @throws QueueException on error
     */
    public abstract Object createPullTransaction()
            throws QueueException;

    /**
     * Selects the underlaying base queue of this queue. If there is no base queue,
     * this queue instance will be returned.
     *
     * @return base queue
     */
    public AbstractQueue selectBaseQueue() {
        return this;
    }

    /**
     * Prepares the transaction with the given transaction id. Messages are
     * stored in the queue (on disk when persistent) but not unlocked. The preparation
     * is logged under the global transaction id.
     *
     * @param localtransactionId  local transaction id
     * @param globalTransactionId global transaction id
     * @throws QueueException on error
     */
    public abstract void prepare(Object localtransactionId, XidImpl globalTransactionId)
            throws QueueException;

    /**
     * Commits a prepared transaction.
     *
     * @param localtransactionId  local transaction id
     * @param globalTransactionId global transaction id
     * @throws QueueException on error
     */
    public abstract void commit(Object localtransactionId, XidImpl globalTransactionId)
            throws QueueException;

    /**
     * Commit the transaction with the given transaction id
     *
     * @param transactionId transaction id
     * @throws QueueException on error
     */
    public abstract void commit(Object transactionId)
            throws QueueException;

    /**
     * Asynchronously commits the transaction with the given transaction id
     *
     * @param transactionId transaction id
     * @param callback      async completion callback
     */
    public abstract void commit(Object transactionId, AsyncCompletionCallback callback);

    /**
     * Rolls back the transaction with the given transaction id, eventually prepared
     * under a global transaction id. If
     * the flag <code>setRedelivered</code> is set then the JMS properties for
     * redelivery and delivery count of messages pulled within this transaction
     * are updated.
     *
     * @param transactionId       transaction id
     * @param globalTransactionId global transaction id
     * @param setRedelivered      specifies JMS redelivery setting
     * @throws QueueException on error
     */
    public abstract void rollback(Object transactionId, XidImpl globalTransactionId, boolean setRedelivered)
            throws QueueException;

    /**
     * Rolls back the transaction with the given transaction id. If
     * the flag <code>setRedelivered</code> is set then the JMS properties for
     * redelivery and delivery count of messages pulled within this transaction
     * are updated
     *
     * @param transactionId  transaction id
     * @param setRedelivered specifies JMS redelivery setting
     * @throws QueueException on error
     */
    public abstract void rollback(Object transactionId, boolean setRedelivered)
            throws QueueException;

    /**
     * Asynchronously rolls back the transaction with the given transaction id. If
     * the flag <code>setRedelivered</code> is set then the JMS properties for
     * redelivery and delivery count of messages pulled within this transaction
     * are updated
     *
     * @param transactionId  transaction id
     * @param setRedelivered specifies JMS redelivery setting
     * @param callback       async completion callback
     */
    public abstract void rollback(Object transactionId, boolean setRedelivered, AsyncCompletionCallback callback);

    /**
     * Deletes all expired messages from the queue
     *
     * @throws QueueException on error
     */
    public void cleanUpExpiredMessages()
            throws QueueException {
        throw new QueueException("operation is not supported");
    }

    /**
     * Returns the cleanup interval in milliseconds
     *
     * @return interval
     * @throws QueueException on error
     */
    public long getCleanUpInterval()
            throws QueueException {
        return cleanUpInterval;
    }

    /**
     * Sets the cleanup interval.
     *
     * @param cleanUpInterval cleanup interval.
     */
    public void setCleanUpInterval(long cleanUpInterval) {
        this.cleanUpInterval = cleanUpInterval;
    }

    /**
     * Returns the number of messages actually stored in the queue
     *
     * @return number of messages
     * @throws QueueException on error
     */
    public long getNumberQueueMessages()
            throws QueueException {
        return (0); // NYI
    }

    /**
     * Returns the consuming rate in Msgs/Sec
     *
     * @return consuming rate
     */
    public int getConsumingRate() {
        return (0); // NYI
    }

    /**
     * Returns the producing rate in Msgs/Sec
     *
     * @return producing rate
     */
    public int getProducingRate() {
        return (0); // NYI
    }

    /**
     * Returns the total number of consumed messages
     *
     * @return total number of consumed messages
     */
    public int getConsumedTotal() {
        return (0); // NYI
    }

    /**
     * Returns the total number of produced messages
     *
     * @return total number of produced messages
     */
    public int getProducedTotal() {
        return (0); // NYI
    }

    /**
     * Resets all consumed/produced counters
     */
    public void resetCounters() {
    }

    public int getMonitorAlertThreshold() {
        return -1;
    }

    /**
     * Get a message from the queue.
     * The method returns the next available message dependend on priority and
     * message time stamp. If no message is available (queue empty) then this
     * method blocks until a message becomes available.
     *
     * @param transactionId
     * @return The message
     * @throws QueueException on error
     */
    public MessageEntry getMessage(Object transactionId)
            throws QueueException {
        throw new QueueException("operation is not supported");
    }

    /**
     * Get a message from the queue that matches the selector.
     * The method returns the next available message dependend on priority and
     * message time stamp. If no message is available (queue empty) then this
     * method blocks until a message becomes available.
     *
     * @param transactionId a valid pull transaction id
     * @param selector      A message selector
     * @return The message
     * @throws QueueException on error
     */
    public MessageEntry getMessage(Object transactionId, Selector selector)
            throws QueueException {
        throw new QueueException("operation is not supported");
    }

    /**
     * Get the next message from a view.
     * The method returns the next available message dependend on priority and
     * message time stamp. If no message is available (queue empty) then this
     * method blocks until a message becomes available.
     *
     * @param transactionId a valid pull transaction id
     * @param selector      a message selector
     * @param viewId        view id
     * @return The message
     * @throws QueueException on error
     */
    public MessageEntry getMessage(Object transactionId, Selector selector, int viewId)
            throws QueueException {
        throw new QueueException("operation is not supported");
    }

    /**
     * Get a message from the queue but wait only a specific period of time.
     * The method returns the next available message dependend on priority and
     * message time stamp. If no message is available (queue empty) then this
     * method blocks until a message becomes available or a timeout occurs.
     *
     * @param transactionId a valid pull transaction id
     * @param timeout       a timeout in ms
     * @return The message
     * @throws QueueException        on error
     * @throws QueueTimeoutException if a timeout occurs
     */
    public MessageEntry getMessage(Object transactionId, long timeout)
            throws QueueException, QueueTimeoutException {
        throw new QueueException("operation is not supported");
    }

    /**
     * Get the next expired message from the queue but wait only a specific period of time.
     * The method returns the next available expired message dependend on priority and
     * message time stamp. If no expired message is available then this
     * method blocks until a message becomes available or a timeout occurs.
     *
     * @param transactionId a valid pull transaction id
     * @param timeout       a timeout in ms
     * @return The message
     * @throws QueueException        on error
     * @throws QueueTimeoutException if a timeout occurs
     */
    public MessageEntry getExpiredMessage(Object transactionId, long timeout)
            throws QueueException, QueueTimeoutException {
        throw new QueueException("operation is not supported");
    }

    /**
     * Get a message from the queue that matches the selector but wait only a specific period of time.
     * The method returns the next available message dependend on priority and
     * message time stamp. If no message is available (queue empty) then this
     * method blocks until a message becomes available or a timeout occurs.
     *
     * @param transactionId a valid pull transaction id
     * @param selector      a message selector
     * @param timeout       a timeout in ms
     * @return The message
     * @throws QueueException        on error
     * @throws QueueTimeoutException if a timeout occurs
     */
    public MessageEntry getMessage(Object transactionId, Selector selector, long timeout)
            throws QueueException, QueueTimeoutException {
        throw new QueueException("operation is not supported");
    }

    /**
     * Get the next message from the view but wait only a specific period of time.
     * The method returns the next available message dependend on priority and
     * message time stamp. If no message is available (queue empty) then this
     * method blocks until a message becomes available or a timeout occurs.
     *
     * @param transactionId a valid pull transaction id
     * @param selector      a message selector
     * @param viewId        view Id
     * @param timeout       a timeout in ms
     * @return The message
     * @throws QueueException        on error
     * @throws QueueTimeoutException if a timeout occurs
     */
    public MessageEntry getMessage(Object transactionId, Selector selector, int viewId, long timeout)
            throws QueueException, QueueTimeoutException {
        throw new QueueException("operation is not supported");
    }

    /**
     * Registers a message processor.
     * The message processor will be called asynchrounsly when a message is available.
     *
     * @param messageProcessor message processor.
     */
    public void registerMessageProcessor(MessageProcessor messageProcessor) {
    }

    /**
     * Unregisters a message processor.
     *
     * @param messageProcessor message processor.
     */
    public void unregisterMessageProcessor(MessageProcessor messageProcessor) {
    }

    /**
     * Removes a message processor which has been timed out.
     *
     * @param registrationTime registration time.
     * @param id               id of the message processor.
     */
    public void timeoutMessageProcessor(long registrationTime, int id) {
    }

    /**
     * Async acknowledges a message that was fetched within a pull transaction. The size of the message will be returned as a result of the callback.
     * This is another way to commit a pull transaction because it commits a single message of the transaction.
     * All other messages of the transaction remain locked.
     *
     * @param transactionId a valid pull transaction id
     * @param messageIndex  a valid message index
     * @param callback      async completion callback
     */
    public void acknowledgeMessage(Object transactionId, MessageIndex messageIndex, AsyncCompletionCallback callback) {
    }

    /**
     * Async acknowledges a list of messages that were fetched within a pull transaction. The size of the messages will be returned as a result of the callback.
     *
     * @param transactionId    a valid pull transaction id
     * @param messageIndexList a list of valid message indexes
     * @param callback         async completion callback
     */
    public void acknowledgeMessages(Object transactionId, List messageIndexList, AsyncCompletionCallback callback) {
    }

    /**
     * Acknowledges a message that was fetched within a pull transaction. This is another
     * way to commit a pull transaction because it commits a single message of the transaction.
     * All other messages of the transaction remain locked.
     *
     * @param transactionId a valid pull transaction id
     * @param messageIndex  a valid message index
     * @throws QueueException on error
     */
    public void acknowledgeMessage(Object transactionId, MessageIndex messageIndex)
            throws QueueException {
        throw new QueueException("operation is not supported");
    }

    /**
     * Moves a message with the given message index from the source to the destination transaction and returns its size.
     *
     * @param messageIndex message index.
     * @param sourceTxId   source transaction id.
     * @param destTxId     destination transaction id.
     * @return size of the corresponding message in bytes
     * @throws QueueException on error.
     */
    public long moveToTransactionReturnSize(MessageIndex messageIndex, Object sourceTxId, Object destTxId)
            throws QueueException {
        throw new QueueException("operation is not supported");
    }

    /**
     * Moves a message with the given message index from the source to the destination transaction.
     *
     * @param messageIndex message index.
     * @param sourceTxId   source transaction id.
     * @param destTxId     destination transaction id.
     * @throws QueueException on error.
     */
    public void moveToTransaction(MessageIndex messageIndex, Object sourceTxId, Object destTxId)
            throws QueueException {
        moveToTransactionReturnSize(messageIndex, sourceTxId, destTxId);
    }

    /**
     * Moves a message with the given message index to the destination transaction and returns the message size. The source transaction
     * is determined by the message index.
     *
     * @param messageIndex message index.
     * @param destTxId     destination transaction id.
     * @return size of the corresponding message in bytes
     * @throws QueueException on error.
     */
    public long moveToTransactionReturnSize(MessageIndex messageIndex, Object destTxId)
            throws QueueException {
        throw new QueueException("operation is not supported");
    }

    /**
     * Moves a message with the given message index to the destination transaction. The source transaction
     * is determined by the message index.
     *
     * @param messageIndex message index.
     * @param destTxId     destination transaction id.
     * @throws QueueException on error.
     */
    public void moveToTransaction(MessageIndex messageIndex, Object destTxId)
            throws QueueException {
        moveToTransactionReturnSize(messageIndex, destTxId);
    }

    /**
     * Checks all registered receivers and, if set, their message selectors
     * whether this message will be received by one of them.
     *
     * @param message
     * @return has receiver or not
     */
    public abstract boolean hasReceiver(MessageImpl message);

    /**
     * Put a message into the queue
     *
     * @param transactionId a valid push transaction id
     * @param message       the message
     * @throws QueueException on error
     */
    public abstract void putMessage(Object transactionId, MessageImpl message)
            throws QueueException;

    /**
     * Remove messages from a queue
     *
     * @param transactionId  a valid push transaction id
     * @param messageIndexes List of MessageIndexes to remove
     * @throws QueueException on error
     * @Since 9.8.0
     */
    public void removeMessages(Object transactionId, List<MessageIndex> messageIndexes)
            throws QueueException {
        throw new QueueException("operation is not supported");
    }

    /**
     * Returns a current snapshot of the queue index (message indexes)
     *
     * @return queue index
     * @throws QueueException on error
     */
    public SortedSet getQueueIndex()
            throws QueueException {
        throw new QueueException("operation is not supported");
    }

    /**
     * Returns a current snapshot of the view
     *
     * @param viewId the view id
     * @return queue index
     * @throws QueueException on error
     */
    public SortedSet getQueueIndex(int viewId)
            throws QueueException {
        throw new QueueException("operation is not supported");
    }

    /**
     * Returns the message with that given key. If the message is not available anymore, the
     * method returns null.
     *
     * @param messageIndex message index
     * @return message entry
     * @throws QueueException on error
     */
    public MessageEntry getMessageByIndex(MessageIndex messageIndex)
            throws QueueException {
        throw new QueueException("operation is not supported");
    }

    /**
     * Removes a message with that given key outside a queue transaction.
     *
     * @param messageIndex
     * @throws QueueException if the message is locked by another consumer
     */
    public void removeMessageByIndex(MessageIndex messageIndex)
            throws QueueException {
        throw new QueueException("operation is not supported");
    }

    /**
     * Returns the actual index entry (the implementation of the MessageIndex class)
     * used for this MessageIndex
     *
     * @param messageIndex
     * @return index entry
     * @throws QueueException on error
     */
    public MessageIndex getIndexEntry(MessageIndex messageIndex)
            throws QueueException {
        throw new QueueException("operation is not supported");
    }

    /**
     * Removes all messages from the queue.
     *
     * @throws QueueException on error
     */
    public void deleteContent()
            throws QueueException {
        throw new QueueException("operation is not supported");
    }

    /**
     * Locks this queue and holds it until unlockQueue is called.
     *
     * @param txId local transactionId
     */
    public void lockQueue(Object txId) {
    }

    /**
     * Unlocks this queue.
     *
     * @param txId            local transactionId
     * @param markAsyncActive blocks queue as async is active
     */
    public void unlockQueue(Object txId, boolean markAsyncActive) {
    }

    /**
     * Unblocks this queue from async active
     *
     * @param txId local transactionId
     */
    public void unmarkAsyncActive(Object txId) {
    }

    /**
     * Sets a CompositeStoreTransaction to be used as the current transaction.
     * At the same time it disables prepare/commit/rollback calls inside this queue
     * so that multiple queues can use the same transaction and prepare/commit/rollback
     * is done outside.
     *
     * @param txId local transactionId
     * @param ct   composite transaction
     */
    public void setCompositeStoreTransaction(Object txId, CompositeStoreTransaction ct) {
    }

    /**
     * Returns the current composite store transaction
     *
     * @param txId local transactionId
     * @return composite transaction
     */
    public CompositeStoreTransaction getCompositeStoreTransaction(Object txId) {
        return null;
    }

}

