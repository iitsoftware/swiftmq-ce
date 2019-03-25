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

import com.swiftmq.tools.gc.Recyclable;
import com.swiftmq.tools.gc.Recycler;

/**
 * A QueueTransactionHandler manages open transactions. It serves as an abstract base
 * class for QueueSender and QueueReceiver.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public abstract class QueueTransactionHandler extends QueueHandler {
    Recycler recycler = null;

    /**
     * Creates a new QueueTransactionHandler
     *
     * @param abstractQueue the queue
     */
    QueueTransactionHandler(AbstractQueue abstractQueue) {
        super(abstractQueue);
    }

    Recycler getRecycler() {
        return recycler;
    }

    void setRecycler(Recycler recycler) {
        this.recycler = recycler;
    }

    /**
     * Closes the QueueTransactionHandler. All open transaction will be rolled back
     * and removed from the handler
     *
     * @throws QueueException              thrown by the queue
     * @throws QueueHandlerClosedException if the handler already was closed
     */
    public void close()
            throws QueueException, QueueHandlerClosedException {
        verifyQueueHandlerState();
        super.close();
        Recyclable[] ta = recycler.getUseList();
        for (int i = 0; i < ta.length; i++) {
            QueueTransaction t = (QueueTransaction) ta[i];
            if (t != null && !t.isClosed() && !t.isPrepared() && t.isDoRollbackOnClose()) {
                try {
                    t.rollback();
                } catch (Exception e) {
                }
            }
        }
        recycler.clear();
    }
}

