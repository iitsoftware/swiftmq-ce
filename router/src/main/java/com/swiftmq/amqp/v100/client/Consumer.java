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

package com.swiftmq.amqp.v100.client;

import com.swiftmq.amqp.v100.client.po.POFillCache;
import com.swiftmq.amqp.v100.client.po.POSendDisposition;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.DeliveryStateIF;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TxnIdIF;
import com.swiftmq.amqp.v100.generated.transport.performatives.TransferFrame;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>A message consumer, created from a session.
 * </p>  <p>
 * A consumer has a client side cache which is asynchronously filled. The cache size is &lt;linkcredit&gt; messages.
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class Consumer extends Link {
    private static final int DEFAULT_LINKCREDIT = 500;
    String source;
    List cache = new ArrayList();
    Lock cacheLock = new ReentrantLock();
    Condition cacheEmpty = null;
    volatile int linkCredit = 0;
    int currentLinkCredit = 0;
    AtomicLong deliveryCount = null;
    boolean acquireMode = false;
    volatile TxnIdIF currentTx = null;
    boolean firstFillCache = true;
    TransferFrame currentMessage = null;
    MessageAvailabilityListener messageAvailabilityListener = null;

    protected Consumer(Session mySession, String source, String name, int linkCredit, int qoS, DeliveryMemory deliveryMemory) {
        super(mySession, name, qoS, deliveryMemory);
        this.source = source;
        this.linkCredit = linkCredit;
        cacheEmpty = cacheLock.newCondition();
        fillCache(-1);
    }

    protected Consumer(Session mySession, String source, String name, int qoS, DeliveryMemory deliveryMemory) {
        super(mySession, name, qoS, deliveryMemory);
        this.source = source;
        this.linkCredit = 0;
        this.acquireMode = true;
        cacheEmpty = cacheLock.newCondition();
    }

    /**
     * Return the source (e.g. name of the queue/topic)
     *
     * @return source
     */
    public String getSource() {
        return source;
    }

    /**
     * Returns the link credit.
     *
     * @return link credit
     */
    public int getLinkCredit() {
        return linkCredit;
    }

    /**
     * Sets the link credit. Default is 500 messages.
     *
     * @param linkCredit link credit
     */
    public void setLinkCredit(int linkCredit) {
        this.linkCredit = linkCredit;
    }

    protected TransferFrame getCurrentMessage() {
        return currentMessage;
    }

    protected void setCurrentMessage(TransferFrame currentMessage) {
        this.currentMessage = currentMessage;
    }

    protected void setDeliveryCount(long deliveryCount) {
        this.deliveryCount = new AtomicLong(deliveryCount);
    }

    protected void fillCache(long lastDeliveryId) {
        cacheLock.lock();
        try {
            if (linkCredit == 0)
                linkCredit = DEFAULT_LINKCREDIT;
            currentLinkCredit = linkCredit;
            mySession.dispatch(new POFillCache(this, linkCredit, lastDeliveryId, currentTx));
            firstFillCache = false;
        } finally {
            cacheLock.unlock();
        }
    }

    protected void addToCache(AMQPMessage message) {
        cacheLock.lock();
        try {
            cache.add(message);
            if (cache.size() == 1) {
                if (messageAvailabilityListener != null) {
                    messageAvailabilityListener.messageAvailable(this);
                    messageAvailabilityListener = null;
                }
                cacheEmpty.signal();
            }
        } finally {
            cacheLock.unlock();
        }
    }

    /**
     * <p>** Internal use only! Will be called from the AMQPMessage accept()/reject() methods **
     * </p>
     * <p>Sends disposition for a message. This is required for all quality of service modes except at-most-once.
     * </p>
     *
     * @param message         the message
     * @param deliveryStateIF the delivery state
     */
    public void sendDisposition(AMQPMessage message, DeliveryStateIF deliveryStateIF) {
        mySession.dispatch(new POSendDisposition(this, message.getDeliveryId(), message.getDeliveryTag(), deliveryStateIF));
    }

    /**
     * Acquirement of the next &lt;linkcredit&gt; messages to deliver it within the same transaction specified by "txnid".
     *
     * @param linkCredit link credit (number of messages to require)
     * @param txnid      transaction id
     * @throws LinkClosedException if the link is closed
     */
    public void acquire(int linkCredit, TxnIdIF txnid) throws LinkClosedException {
        verifyState();
        this.linkCredit = linkCredit;
        currentTx = txnid;
        fillCache(firstFillCache ? -1 : deliveryCount.get());
    }

    /**
     * Receive a message with a timeout. A timeout of 0 means no timeout and block (waits until a message is available).
     * A value of -1 means no timeout and non-block (returns immediately with or without a message). If the consumer is
     * closed and was in a receive call, the receive will be released and returns null.
     *
     * @param timeout timeout
     * @return message or null
     */
    public AMQPMessage receive(long timeout) {
        return receive(timeout, null);
    }

    private AMQPMessage receive(long timeout, MessageAvailabilityListener messageAvailabilityListener) {
        AMQPMessage msg = null;
        cacheLock.lock();
        if (closed)
            return null;
        try {
            if (cache.size() == 0) {
                if (timeout > 0) {
                    try {
                        cacheEmpty.await(timeout, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                    }
                } else {
                    if (timeout == 0)
                        cacheEmpty.awaitUninterruptibly();
                    else if (timeout == -1)
                        this.messageAvailabilityListener = messageAvailabilityListener;
                }
            }
            if (cache.size() == 0)
                return null;
            msg = (AMQPMessage) cache.remove(0);
            msg.setConsumer(this);
            long dc = deliveryCount.incrementAndGet();
            currentLinkCredit--;
            if (!acquireMode && currentLinkCredit == 0)
                fillCache(dc);
        } finally {
            cacheLock.unlock();
        }
        return msg;
    }

    /**
     * A blocking receive.
     *
     * @return message
     */
    public AMQPMessage receive() {
        return receive(0);
    }

    /**
     * A nonblocking receive.
     *
     * @return message.
     */
    public AMQPMessage receiveNoWait() {
        return receive(-1);
    }

    /**
     * A nonblocking receive. If no message is available, it registers the MessageAvailabilityListener and returns null.
     *
     * @param messageAvailabilityListener MessageAvailabilityListener
     * @return message
     */
    public AMQPMessage receiveNoWait(MessageAvailabilityListener messageAvailabilityListener) {
        return receive(-1, messageAvailabilityListener);
    }

    public void close() throws AMQPException {
        cacheLock.lock();
        try {
            cacheEmpty.signal();
        } finally {
            cacheLock.unlock();
        }
        super.close();
    }

    protected void cancel() {
        super.cancel();

        cacheLock.lock();
        try {
            cacheEmpty.signal();
        } finally {
            cacheLock.unlock();
        }
    }
}