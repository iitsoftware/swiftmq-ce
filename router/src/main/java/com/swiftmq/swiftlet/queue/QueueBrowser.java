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

import com.swiftmq.mgmt.EntityList;

import java.util.Iterator;
import java.util.SortedSet;

/**
 * A QueueBrowser is created by the QueueManager. It provides a method for
 * browsing queue messages outside a transaction.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 * @see QueueManager#createQueueBrowser
 */
public class QueueBrowser extends QueueHandler {
    Selector selector = null;
    SortedSet queueIndex = null;
    MessageIndex lastMessageIndex = null;
    EntityList browserEntityList = null;
    int viewId = -1;

    /**
     * Creates a new QueueBrowser
     *
     * @param activeQueue       the active queue
     * @param selector          an optional selector
     * @param browserEntityList the browser usage entity list
     */
    public QueueBrowser(ActiveQueue activeQueue, Selector selector, EntityList browserEntityList) {
        super(activeQueue.getAbstractQueue());
        this.selector = selector;
        this.browserEntityList = browserEntityList;
    }

    public void setLastMessageIndex(MessageIndex lastMessageIndex) {
        this.lastMessageIndex = lastMessageIndex;
    }

    private MessageIndex getNextEntry(MessageIndex storeId) {
        MessageIndex rMessageIndex = null;
        if (storeId == null) {
            if (queueIndex.size() > 0)
                rMessageIndex = (MessageIndex) queueIndex.first();
        } else {
            Iterator iterator = queueIndex.iterator();
            while (iterator.hasNext()) {
                MessageIndex s = (MessageIndex) iterator.next();
                if (s.compareTo(storeId) > 0) {
                    rMessageIndex = s;
                    break;
                }
            }
        }
        return rMessageIndex;
    }


    /**
     * Reset the browser.
     * It will start at the beginning next time a message is fetched.
     */
    public void resetBrowser() {
        queueIndex = null;
        lastMessageIndex = null;
        if (viewId != -1)
            abstractQueue.deleteView(viewId);
    }

    /**
     * Get the next available message from the queue.
     *
     * @return message or null
     * @throws QueueException              thrown by the queue
     * @throws QueueHandlerClosedException if the handler is closed
     */
    public synchronized MessageEntry getNextMessage()
            throws QueueException, QueueHandlerClosedException {
        verifyQueueHandlerState();
        MessageEntry me = null;
        if (queueIndex == null) {
            if (selector == null)
                queueIndex = abstractQueue.getQueueIndex();
            else {
                viewId = abstractQueue.createView(selector);
                queueIndex = abstractQueue.getQueueIndex(viewId);
            }
        }
        boolean found = false;
        while (!found) {
            MessageIndex s = getNextEntry(lastMessageIndex);
            if (s == null)
                found = true;
            else {
                lastMessageIndex = s;
                MessageEntry m = abstractQueue.getMessageByIndex(s);
                if (m != null) {
                    me = m;
                    found = true;
                }
            }
        }
        return me;
    }

    /**
     * Close the queue browser
     *
     * @throws QueueException              thrown by the queue
     * @throws QueueHandlerClosedException if the browser is already closed
     */
    public void close()
            throws QueueException, QueueHandlerClosedException {
        super.close();
        if (browserEntityList != null) {
            browserEntityList.removeDynamicEntity(this);
            browserEntityList = null;
        }
        queueIndex = null;
        lastMessageIndex = null;
        if (viewId != -1)
            abstractQueue.deleteView(viewId);
    }
}

