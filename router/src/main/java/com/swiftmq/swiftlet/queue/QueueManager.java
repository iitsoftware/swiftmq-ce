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

import com.swiftmq.swiftlet.Swiftlet;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.auth.AuthenticationException;
import com.swiftmq.swiftlet.queue.event.QueueManagerListener;

import java.util.concurrent.locks.ReentrantLock;

/**
 * The QueueManager manages queues.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public abstract class QueueManager extends Swiftlet {
    protected ReentrantLock multiQueueLock = new ReentrantLock();
    protected boolean useGlobalLocking = false;

    /**
     * Applies the global lock for multi queue transactions
     */
    public void lockMultiQueue() {
        multiQueueLock.lock();
    }

    /**
     * Releases the global lock for multi queue transactions
     */
    public void unlockMultiQueue() {
        multiQueueLock.unlock();
    }

    /**
     * States whether global locking should be used for multi queue transactions
     *
     * @return true/false
     */
    public boolean isUseGlobaLocking() {
        return useGlobalLocking;
    }

    /**
     * Sets the global locking flag
     *
     * @param useGlobalLocking global locking flag
     */
    protected void setUseGlobalLocking(boolean useGlobalLocking) {
        this.useGlobalLocking = useGlobalLocking;
    }

    /**
     * Returns wheter a queue with that name is defined.
     *
     * @param queueName queue name.
     * @return true/false.
     */
    public abstract boolean isQueueDefined(String queueName);

    /**
     * Returns wheter a queue with that name is running.
     *
     * @param queueName queue name.
     * @return true/false.
     */
    public abstract boolean isQueueRunning(String queueName);

    /**
     * Returns the given queue (internal use only!).
     *
     * @param queueName queue name.
     * @return queue.
     */
    public abstract AbstractQueue getQueueForInternalUse(String queueName);

    /**
     * Creates a QueueSender.
     *
     * @param queueName   queue name.
     * @param activeLogin active login (Swiftlets pass null here).
     * @return queue sender.
     * @throws QueueException          on error.
     * @throws AuthenticationException thrown by the AuthenticationSwiftlet.
     * @throws UnknownQueueException   if the queue is undefined.
     */
    public abstract QueueSender createQueueSender(String queueName, ActiveLogin activeLogin)
            throws QueueException, AuthenticationException, UnknownQueueException;

    /**
     * Creates a QueueReceiver
     *
     * @param queueName   queue name.
     * @param activeLogin active login (Swiftlets pass null here).
     * @param selector    selector.
     * @return queue receiver.
     * @throws QueueException          on error.
     * @throws AuthenticationException thrown by the AuthenticationSwiftlet.
     * @throws UnknownQueueException   if the queue is undefined.
     */
    public abstract QueueReceiver createQueueReceiver(String queueName, ActiveLogin activeLogin, Selector selector)
            throws QueueException, AuthenticationException, UnknownQueueException;

    /**
     * Creates a QueueBrowser.
     *
     * @param queueName   queue name.
     * @param activeLogin active login (Swiftlets pass null here).
     * @param selector    selector.
     * @return queue browser.
     * @throws QueueException          on error.
     * @throws AuthenticationException thrown by the AuthenticationSwiftlet.
     * @throws UnknownQueueException   if the queue is undefined.
     */
    public abstract QueueBrowser createQueueBrowser(String queueName, ActiveLogin activeLogin, Selector selector)
            throws QueueException, AuthenticationException, UnknownQueueException;

    /**
     * Adds a queue manager listener for all queues.
     *
     * @param l listener.
     */
    public abstract void addQueueManagerListener(QueueManagerListener l);

    /**
     * Adds a queue manager listener for a distinct queue.
     *
     * @param queueName queue name.
     * @param l         listener.
     * @throws UnknownQueueException if the queue is undefined.
     */
    public abstract void addQueueManagerListener(String queueName, QueueManagerListener l)
            throws UnknownQueueException;

    /**
     * Removes a queue manager listener for all queues.
     *
     * @param l listener.
     */
    public abstract void removeQueueManagerListener(QueueManagerListener l);

    /**
     * Removes a queue manager listener for a distinct queue.
     *
     * @param queueName queue name.
     * @param l         listener.
     * @throws UnknownQueueException if the queue is undefined.
     */
    public abstract void removeQueueManagerListener(String queueName, QueueManagerListener l)
            throws UnknownQueueException;

    /**
     * Creates a queue.
     *
     * @param queueName   queue name.
     * @param activeLogin active login (Swiftlets pass null here).
     * @throws QueueException               on error.
     * @throws QueueAlreadyDefinedException if the queue is already defined.
     * @throws AuthenticationException      thrown by the AuthenticationSwiftlet.
     */
    public abstract void createQueue(String queueName, ActiveLogin activeLogin)
            throws QueueException, QueueAlreadyDefinedException, AuthenticationException;

    /**
     * Creates a queue from a specified queue factory.
     *
     * @param queueName queue name.
     * @param factory   queue factory.
     * @throws QueueException               on error.
     * @throws QueueAlreadyDefinedException if the queue is already defined.
     */
    public abstract void createQueue(String queueName, QueueFactory factory)
            throws QueueException, QueueAlreadyDefinedException;

    /**
     * Deletes a queue.
     *
     * @param queueName queue name.
     * @param onEmpty   true: queue will only be deleted if empty.
     * @throws UnknownQueueException if the queue is undefined.
     * @throws QueueException        on error.
     */
    public abstract void deleteQueue(String queueName, boolean onEmpty)
            throws UnknownQueueException, QueueException;

    /**
     * Create a temporary queue.
     *
     * @return queue name.
     * @throws QueueException on error.
     */
    public abstract String createTemporaryQueue()
            throws QueueException;

    /**
     * Delete a temporary queue.
     *
     * @param queueName queue name.
     * @throws UnknownQueueException if the queue is undefined.
     * @throws QueueException        on error.
     */
    public abstract void deleteTemporaryQueue(String queueName)
            throws UnknownQueueException, QueueException;

    /**
     * Purges a queue (deletes the content).
     *
     * @param queueName queue name.
     * @throws UnknownQueueException if the queue is undefined.
     * @throws QueueException        on error.
     */
    public abstract void purgeQueue(String queueName)
            throws UnknownQueueException, QueueException;

    /**
     * Returns the defined queue names.
     *
     * @return defined queue names.
     */
    public abstract String[] getDefinedQueueNames();

    /**
     * Sets an outbound redirector.
     * This maps a SQL Like predicate to a queue name. This predicate is checked on
     * queue sender creation and, if it matches for the queue name the sender should be
     * created for, it will redirected to the queue specified for this predicate.
     * Example: If <code>%@router2</code> is the predicate and the outbound queue name
     * is <code>rt$router2</code>, a queue sender for <code>testqueue@router2</code>
     * is redirected to queue <code>rt$router2</code> which is the routing queue for
     * router2.
     *
     * @param likePredicate     predicate.
     * @param outboundQueueName queue name.
     * @throws UnknownQueueException if the queue is undefined.
     */
    public abstract void setQueueOutboundRedirector(String likePredicate, String outboundQueueName)
            throws UnknownQueueException;

    /**
     * Sets an inbound redirector.
     * This maps a SQL Like predicate to a queue name. This predicate is checked on
     * queue receiver creation and, if it matches for the queue name the receiver should be
     * created for, it will redirected to the queue specified for this predicate.
     *
     * @param likePredicate     predicate.
     * @param outboundQueueName queue name.
     * @throws UnknownQueueException if the queue is undefined.
     */
    public abstract void setQueueInboundRedirector(String likePredicate, String inboundQueueName)
            throws UnknownQueueException;

    /**
     * Returns whether the queue is a temp. queue.
     *
     * @param queueName queue name.
     * @return true/false.
     */
    public abstract boolean isTemporaryQueue(String queueName);

    /**
     * Returns whether the queue is a system queue.
     *
     * @param queueName queue name.
     * @return true/false.
     */
    public abstract boolean isSystemQueue(String queueName);

    /**
     * Returns the maximum flow control delay in milliseconds
     *
     * @return max flow control delay
     */
    public abstract long getMaxFlowControlDelay();

}



