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
 * A QueueSender is a QueueTransactionHandler that serves as a factory
 * for QueuePushTransactions. Pushing messages into the underlying queue
 * goes via QueuePushTransactions. QueueSenders are created by the
 * QueueManager.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 * @see QueueManager#createQueueSender
 */
public class QueueSender extends QueueTransactionHandler {
    EntityList senderEntityList = null;

    /**
     * Creates a new QueueSender
     *
     * @param activeQueue the queue
     */
    public QueueSender(ActiveQueue activeQueue, EntityList senderEntityList) {
        super(activeQueue.getAbstractQueue());
        setRecycler(new PushTransactionRecycler());
        this.senderEntityList = senderEntityList;
    }

    /**
     * Creates a new QueuePushTransaction.
     *
     * @return QueuePushTransaction
     * @throws QueueException              thrown by the queue
     * @throws QueueHandlerClosedException if the handler is closed
     */
    public QueuePushTransaction createTransaction()
            throws QueueException, QueueHandlerClosedException {
        verifyQueueHandlerState();
        QueuePushTransaction t = (QueuePushTransaction) recycler.checkOut();
        t.restart(abstractQueue,
                abstractQueue.createPushTransaction(),
                this);
        return t;
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
     * Close the queue sender
     *
     * @throws QueueException              throws by the queue
     * @throws QueueHandlerClosedException if the browser is already closed
     */
    public void close()
            throws QueueException, QueueHandlerClosedException {
        super.close();
        if (senderEntityList != null) {
            senderEntityList.removeDynamicEntity(this);
            senderEntityList = null;
        }
    }

    private class PushTransactionRecycler extends Recycler {
        protected Recyclable createRecyclable() {
            return new QueuePushTransaction();
        }
    }
}

