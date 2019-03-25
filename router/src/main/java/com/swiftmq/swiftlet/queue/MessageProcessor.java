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

import com.swiftmq.swiftlet.threadpool.AsyncTask;

/**
 * Abstract MessageProcessor to register at a <code>QueuePullTransaction</code>.
 * The <code>QueuePullTransaction</code> registers it at the queue within the
 * transaction context. The MessageProcessor will be invoked when a message
 * is available in the queue and if it does match the selector, if specified.
 * <br><br>
 * The MessageProcessor can be used in bulk mode. That is, the
 * <code>setBulkMode(true)</code> must be called and the bulk buffer has
 * to be created with <code>ceateBulkBuffer(size)</code>. In bulk mode, the
 * method <code>processMessages(n)</code> is called instead of
 * <code>processMessage(messageEntry)</code>. The size of the bulk can be
 * limited by overwritin method <code>getMaxBulkSize()</code> which returns
 * either -1 (unlimited) or the max size in bytes. The Queue Manager will
 * <code>setCurrentBulkSize(n)</code> zu the size of all messages in the current bulk.
 * <br><br>
 * MessageProcessor implements <code>AsyncTask</code> for convenience. It is
 * highly recommended to implement the processing of the message inside the
 * <code>run()</code> method and to dispatch this task in a thread pool within
 * <code>processMessage()</code> to avoid blocking of other threads.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public abstract class MessageProcessor
        implements AsyncTask {
    Object transactionId = null;
    Selector selector = null;
    long timeout = -1;
    long registrationTime = -1;
    boolean autoCommit = false;
    boolean bulkMode = false;
    MessageEntry[] bulkBuffer = null;
    long currentBulkSize = 0;
    int viewId = -1;
    int registrationId = -1;
    long receiverId = -1;

    /**
     * Creates a new MessageProcessor.
     */
    protected MessageProcessor() {
        this(null, 0);
    }

    /**
     * Creates a new MessageProcessor with a selector.
     *
     * @param selector selector.
     */
    protected MessageProcessor(Selector selector) {
        this(selector, 0);
    }

    /**
     * Creates a new MessageProcessor with a selector and timeout.
     * If a timeout occurs, <code>processException()</code> is called and
     * the exception will be a <code>QueueTimeoutException</code>
     *
     * @param selector selector.
     * @param timeout  timeout (0 = no timeout).
     * @see QueueTimeoutException
     */
    protected MessageProcessor(Selector selector, long timeout) {
        this.selector = selector;
        this.timeout = timeout;
    }

    public long getReceiverId() {
        return receiverId;
    }

    public void setReceiverId(long receiverId) {
        this.receiverId = receiverId;
    }

    /**
     * Sets the transaction id.
     * Called from the <code>QueuePullTransaction</code>
     *
     * @param transactionId transaction id.
     */
    protected void setTransactionId(Object transactionId) {
        this.transactionId = transactionId;
    }

    /**
     * Returns the transaction id.
     *
     * @return transaction id.
     */
    public Object getTransactionId() {
        return (transactionId);
    }

    /**
     * Sets a registration time.
     * Internal use for timeout processing.
     *
     * @param registrationTime registration time.
     */
    public void setRegistrationTime(long registrationTime) {
        this.registrationTime = registrationTime;
    }

    /**
     * Returns the registration time.
     *
     * @return registration time.
     */
    public long getRegistrationTime() {
        return (registrationTime);
    }

    /**
     * Sets a registration id.
     * Internal use.
     *
     * @param registrationId registration id.
     */
    public void setRegistrationId(int registrationId) {
        this.registrationId = registrationId;
    }

    /**
     * Returns the registration id.
     *
     * @return registration id.
     */
    public int getRegistrationId() {
        return (registrationId);
    }

    /**
     * Returns the selector.
     *
     * @return selector.
     */
    public Selector getSelector() {
        return (selector);
    }

    /**
     * Sets the view id.
     * Internal use.
     *
     * @param viewId registration time.
     */
    public void setViewId(int viewId) {
        this.viewId = viewId;
    }

    /**
     * Returns the viewId.
     *
     * @return view Id.
     */
    public int getViewId() {
        return viewId;
    }

    /**
     * Returns the timeout.
     *
     * @return timeout.
     */
    public long getTimeout() {
        return (timeout);
    }

    /**
     * Returns whether the message should be auto committed
     *
     * @return auto commit.
     */
    public boolean isAutoCommit() {
        return autoCommit;
    }

    /**
     * Sets whether the message should be auto committed
     *
     * @param autoCommit auto commit.
     */
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    /**
     * Returns whether this message processor runs in bulk mode
     *
     * @return bulk mode
     */
    public boolean isBulkMode() {
        return bulkMode;
    }

    /**
     * Sets whether this message processor runs in bulk mode
     *
     * @param bulkMode bulk mode
     */
    protected void setBulkMode(boolean bulkMode) {
        this.bulkMode = bulkMode;
    }

    /**
     * Creates a new bulk buffer in the given size
     *
     * @param bulkSize bulk size
     */
    protected void createBulkBuffer(int bulkSize) {
        bulkBuffer = new MessageEntry[bulkSize];
    }

    /**
     * Returns the bulk buffer
     *
     * @return bulk buffer
     */
    public MessageEntry[] getBulkBuffer() {
        return bulkBuffer;
    }

    /**
     * Returns the max bulk size
     *
     * @return max bulk size
     */
    public long getMaxBulkSize() {
        return -1;
    }

    /**
     * Returns the current bulk
     *
     * @return current bulk size
     */
    public long getCurrentBulkSize() {
        return currentBulkSize;
    }

    /**
     * Sets the current bulk size
     *
     * @param currentBulkSize current bulk size
     */
    public void setCurrentBulkSize(long currentBulkSize) {
        this.currentBulkSize = currentBulkSize;
    }

    /**
     * Process messages in bulk mode.
     * Called when messages are available and the selector matches, if specified.
     * The queue first obtains the bulk buffer with <code>getBulkBuffer()</code>
     * and fills it with message entries up to the buffer size. It then calls this
     * method and passes the actual number of messages in the buffer.
     * Use this method to store the message and dispatch the MessageProcessor to
     * a thread pool. Implement the processing itself inside the <code>run()</code>
     * method.
     *
     * @param numberMessages number Messages in bulk buffer.
     */
    public void processMessages(int numberMessages) {
        throw new RuntimeException("processMessages [bulk mode] not implemented!");
    }

    /**
     * Process a message.
     * Called when a message is available and the selector matches, if specified.
     * Use this method to store the message and dispatch the MessageProcessor to
     * a thread pool. Implement the processing itself inside the <code>run()</code>
     * method.
     *
     * @param messageEntry message entry.
     */
    public abstract void processMessage(MessageEntry messageEntry);

    /**
     * Process an exception.
     * A timeout is indicated by a <code>QueueTimeoutException</code>.
     *
     * @param exception exception.
     * @see QueueTimeoutException
     */
    public abstract void processException(Exception exception);

    public String getDispatchToken() {
        return null;
    }

    public String getDescription() {
        return null;
    }

    public void stop() {
    }

    /**
     * Reset the MessageProcessor to its initial state without the need to recreate it.
     */
    public void reset() {
    }

    public void run() {
    }
}

