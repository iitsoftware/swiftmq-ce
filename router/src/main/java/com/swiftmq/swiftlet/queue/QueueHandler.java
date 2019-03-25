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

import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.queue.event.QueueManagerAdapter;
import com.swiftmq.swiftlet.queue.event.QueueManagerEvent;

/**
 * QueueHandler is an internal base class for QueueBrowsers and QueueTransactionHandler.
 * It is an extension of QueueManagerAdapter and is listening for events if the queue stop
 * is initiated and does the appropriate actions.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class QueueHandler extends QueueManagerAdapter {
    AbstractQueue abstractQueue;
    boolean closed;
    QueueManager queueManager = null;
    String queueName = null;

    /**
     * Constructs a new QueueHandler
     *
     * @param abstractQueue the queue
     */
    QueueHandler(AbstractQueue abstractQueue) {
        this.abstractQueue = abstractQueue;
        queueName = abstractQueue.getQueueName();
        closed = false;
        queueManager = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
    }

    /**
     * Returns the queue name
     *
     * @return queue name
     */
    public String getQueueName() {
        return queueName;
    }

    /**
     * Returns the number of messages in the queue
     *
     * @return number of messages
     * @throws QueueException              thrown by the queue
     * @throws QueueHandlerClosedException thrown by the handler
     */
    public long getNumberQueueMessages()
            throws QueueException, QueueHandlerClosedException {
        verifyQueueHandlerState();
        return abstractQueue.getNumberQueueMessages();
    }

    /**
     * Returns if the QueueHandler is closed
     *
     * @return closed or not
     */
    public boolean isClosed() {
        // SBgen: Get variable
        return (closed);
    }

    /**
     * Close the QueueHandler
     *
     * @throws QueueException              thrown by the queue
     * @throws QueueHandlerClosedException thrown by the handler
     */
    public void close()
            throws QueueException, QueueHandlerClosedException {
        verifyQueueHandlerState();
        try {
            queueManager.removeQueueManagerListener(queueName, this);
        } catch (Exception e) {
        }
        closed = true;
    }

    /**
     * Will be called from the QueueManager if a queue should be stopped.
     * Closes this queue handler.
     *
     * @param evt queue manager event
     */
    public void queueStopInitiated(QueueManagerEvent evt) {
        try {
            close();
        } catch (Exception e) {
        }
    }

    /**
     * Verifies the state of the handler. If the handler is closed, a QueueHandlerClosedException
     * will be thrown
     *
     * @throws QueueHandlerClosedException if the handler is closed
     */
    protected void verifyQueueHandlerState()
            throws QueueHandlerClosedException {
        if (isClosed())
            throw new QueueHandlerClosedException("queue handler is closed!");
    }
}

