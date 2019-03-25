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
import com.swiftmq.tools.gc.Recyclable;
import com.swiftmq.tools.gc.Recycler;

/**
 * A QueueReceiver is a QueueTransactionHandler that serves as a factory
 * for QueuePullTransactions. Pulling messages from the underlying queue
 * goes via QueuePullTransactions. QueueReceivers are created by the
 * QueueManager.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 * @see QueueManager#createQueueReceiver
 */
public class QueueReceiver extends QueueTransactionHandler {
    EntityList receiverEntityList = null;
    Selector selector = null;
    int viewId = -1;
    long receiverId = -1;

    /**
     * Creates a new QueueReceiver
     *
     * @param activeQueue the active queue
     */
    public QueueReceiver(ActiveQueue activeQueue, EntityList receiverEntityList) {
        super(activeQueue.getAbstractQueue().selectBaseQueue());
        setRecycler(new PullTransactionRecycler());
        this.receiverEntityList = receiverEntityList;
        abstractQueue.incReceiverCount();
    }

    /**
     * Creates a new QueueReceiver
     *
     * @param activeQueue the active queue
     */
    public QueueReceiver(ActiveQueue activeQueue, EntityList receiverEntityList, Selector selector) {
        this(activeQueue, receiverEntityList);
        this.selector = selector;
        if (selector != null)
            viewId = abstractQueue.createView(selector);
    }

    public long getReceiverId() {
        return receiverId;
    }

    public void setReceiverId(long receiverId) {
        this.receiverId = receiverId;
    }

    /**
     * Creates a new QueuePullTransaction. The flag <code>setRedeliveredOnRollback</code>
     * specifies whether redelivery setting for all pulled messages should be processed or
     * not. If true then all messages pulled whithin this transaction will be incremented
     * in there delivery count header field and a redelivered flag will be set if the
     * message is pulled again.
     *
     * @param setRedeliveredOnRollback flag
     * @return QueuePullTransaction
     * @throws QueueException              thrown by the queue
     * @throws QueueHandlerClosedException if the handler is closed
     */
    public QueuePullTransaction createTransaction(boolean setRedeliveredOnRollback)
            throws QueueException, QueueHandlerClosedException {
        verifyQueueHandlerState();
        QueuePullTransaction t = (QueuePullTransaction) recycler.checkOut();
        t.restart(abstractQueue,
                abstractQueue.createPullTransaction(),
                this,
                setRedeliveredOnRollback);
        t.setView(selector, viewId);
        t.setReceiverId(receiverId);
        return t;
    }

    /**
     * Close the queue receiver
     *
     * @throws QueueException              thrown by the queue
     * @throws QueueHandlerClosedException if the browser is already closed
     */
    public void close()
            throws QueueException, QueueHandlerClosedException {
        abstractQueue.decReceiverCount();
        abstractQueue.receiverClosed(receiverId);
        super.close();
        if (receiverEntityList != null) {
            receiverEntityList.removeDynamicEntity(this);
            receiverEntityList = null;
        }
        if (viewId != -1)
            abstractQueue.deleteView(viewId);
        viewId = -1;
    }

    private class PullTransactionRecycler extends Recycler {
        protected Recyclable createRecyclable() {
            return new QueuePullTransaction();
        }
    }
}

