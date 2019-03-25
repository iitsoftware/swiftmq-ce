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

package com.swiftmq.impl.queue.standard.cluster;

import com.swiftmq.impl.queue.standard.SwiftletContext;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.XidImpl;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.FlowController;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.store.CompositeStoreTransaction;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;
import com.swiftmq.tools.concurrent.CallbackJoin;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ClusteredTransactionId {
    SwiftletContext ctx = null;
    QueueImpl destination = null;
    AbstractQueue baseQueue = null;
    Object baseTx = null;
    boolean messageBasedDispatch = false;
    Map<String, Entry> queueMap = null;
    CompositeStoreTransaction currentCT = null;

    public ClusteredTransactionId(SwiftletContext ctx, boolean messageBasedDispatch) {
        this.ctx = ctx;
        this.messageBasedDispatch = messageBasedDispatch;
        queueMap = new HashMap<String, Entry>();
    }

    public ClusteredTransactionId(SwiftletContext ctx, AbstractQueue baseQueue, Object baseTx, QueueImpl destination) {
        this.ctx = ctx;
        this.baseQueue = baseQueue;
        this.baseTx = baseTx;
        this.destination = destination;
    }

    public boolean isMessageBasedDispatch() {
        return messageBasedDispatch;
    }

    public void putMessage(String queueName, MessageImpl message) throws Exception {
        Entry entry = queueMap.get(queueName);
        if (entry == null) {
            AbstractQueue abstractQueue = ctx.queueManager.getQueueForInternalUse(queueName);
            QueueImpl destination = new QueueImpl(queueName);
            Object transaction = abstractQueue.createPushTransaction();
            entry = new Entry(abstractQueue, destination, transaction);
            queueMap.put(queueName, entry);
        }
        message.setJMSDestination(entry.destination);
        message.setSourceRouter(null);
        message.setDestRouter(null);
        message.setDestQueue(queueName);
        entry.abstractQueue.putMessage(entry.transaction, message);
    }

    public void putMessage(MessageImpl message) throws Exception {
        message.setJMSDestination(destination);
        message.setSourceRouter(null);
        message.setDestRouter(null);
        message.setDestQueue(destination.getQueueName());
        baseQueue.putMessage(baseTx, message);
    }

    public QueueImpl getDestination() {
        return destination;
    }

    public Object getBaseTx() {
        return baseTx;
    }

    public void lockQueue() {
        if (messageBasedDispatch) {
            for (Iterator iter = queueMap.entrySet().iterator(); iter.hasNext(); ) {
                Entry entry = (Entry) ((Map.Entry) iter.next()).getValue();
                entry.abstractQueue.lockQueue(entry.transaction);
            }
        } else {
            if (baseQueue == null)
                return;
            baseQueue.lockQueue(baseTx);
        }
    }

    public void unlockQueue(boolean markAsyncActive) {
        if (messageBasedDispatch) {
            for (Iterator iter = queueMap.entrySet().iterator(); iter.hasNext(); ) {
                Entry entry = (Entry) ((Map.Entry) iter.next()).getValue();
                entry.abstractQueue.unlockQueue(entry.transaction, markAsyncActive);
            }
        } else {
            if (baseQueue == null)
                return;
            baseQueue.unlockQueue(baseTx, markAsyncActive);
        }
    }

    public void unmarkAsyncActive() {
        if (messageBasedDispatch) {
            for (Iterator iter = queueMap.entrySet().iterator(); iter.hasNext(); ) {
                Entry entry = (Entry) ((Map.Entry) iter.next()).getValue();
                entry.abstractQueue.unmarkAsyncActive(entry.transaction);
            }
        } else {
            if (baseQueue == null)
                return;
            baseQueue.unmarkAsyncActive(baseTx);
        }
    }

    public void setCompositeStoreTransaction(CompositeStoreTransaction ct) {
        if (messageBasedDispatch) {
            currentCT = ct;
        } else {
            if (baseQueue == null)
                return;
            baseQueue.setCompositeStoreTransaction(baseTx, ct);
        }
    }

    public CompositeStoreTransaction getCompositeStoreTransaction() {
        CompositeStoreTransaction ct = null;
        if (messageBasedDispatch) {
            for (Iterator iter = queueMap.entrySet().iterator(); iter.hasNext(); ) {
                Entry entry = (Entry) ((Map.Entry) iter.next()).getValue();
                ct = entry.abstractQueue.getCompositeStoreTransaction(entry.transaction);
                break;
            }
        } else {
            if (baseQueue == null)
                return null;
            ct = baseQueue.getCompositeStoreTransaction(baseTx);
        }
        return ct;
    }

    public void prepare(XidImpl xid) throws QueueException {
        if (messageBasedDispatch) {
            for (Iterator iter = queueMap.entrySet().iterator(); iter.hasNext(); ) {
                Entry entry = (Entry) ((Map.Entry) iter.next()).getValue();
                entry.abstractQueue.prepare(entry.transaction, xid);
            }
        } else {
            if (baseQueue == null)
                return;
            baseQueue.prepare(baseTx, xid);
        }
    }

    public long commit(XidImpl xid) throws QueueException {
        long fcDelay = 0;
        if (messageBasedDispatch) {
            for (Iterator iter = queueMap.entrySet().iterator(); iter.hasNext(); ) {
                Entry entry = (Entry) ((Map.Entry) iter.next()).getValue();
                entry.abstractQueue.commit(entry.transaction, xid);
                FlowController flowController = entry.abstractQueue.getFlowController();
                if (flowController != null)
                    fcDelay = Math.max(fcDelay, flowController.getNewDelay());
            }
        } else {
            if (baseQueue == null)
                return 0;
            baseQueue.commit(baseTx, xid);
            FlowController flowController = baseQueue.getFlowController();
            if (flowController != null)
                fcDelay = Math.max(fcDelay, flowController.getNewDelay());
        }
        return fcDelay;
    }

    public long commit() throws QueueException {
        long fcDelay = 0;
        if (messageBasedDispatch) {
            for (Iterator iter = queueMap.entrySet().iterator(); iter.hasNext(); ) {
                Entry entry = (Entry) ((Map.Entry) iter.next()).getValue();
                entry.abstractQueue.setCompositeStoreTransaction(entry.transaction, currentCT);
                entry.abstractQueue.commit(entry.transaction);
                entry.abstractQueue.setCompositeStoreTransaction(entry.transaction, null);
                FlowController flowController = entry.abstractQueue.getFlowController();
                if (flowController != null)
                    fcDelay = Math.max(fcDelay, flowController.getNewDelay());
            }
        } else {
            if (baseQueue == null)
                return 0;
            baseQueue.commit(baseTx);
            FlowController flowController = baseQueue.getFlowController();
            if (flowController != null)
                fcDelay = Math.max(fcDelay, flowController.getNewDelay());
        }
        return fcDelay;
    }

    public void commit(AsyncCompletionCallback callback) {
        if (messageBasedDispatch) {
            if (queueMap.size() == 0) {
                callback.notifyCallbackStack(true);
                return;
            }
            final DelayCollector delayCollector = new DelayCollector(callback);
            for (Iterator iter = queueMap.entrySet().iterator(); iter.hasNext(); ) {
                Entry entry = (Entry) ((Map.Entry) iter.next()).getValue();
                delayCollector.incNumberCallbacks();
                entry.abstractQueue.commit(entry.transaction, new AsyncCompletionCallback() {
                    public void done(boolean success) {
                        delayCollector.done(this, success);
                    }
                });
            }
            delayCollector.setBlocked(false);
        } else {
            if (baseQueue == null)
                return;
            baseQueue.commit(baseTx, callback);
        }
    }

    public void rollback(XidImpl xid, boolean b) throws QueueException {
        if (messageBasedDispatch) {
            for (Iterator iter = queueMap.entrySet().iterator(); iter.hasNext(); ) {
                Entry entry = (Entry) ((Map.Entry) iter.next()).getValue();
                entry.abstractQueue.rollback(entry.transaction, xid, b);
            }
        } else {
            if (baseQueue == null)
                return;
            baseQueue.rollback(baseTx, xid, b);
        }
    }

    public void rollback(boolean b) throws QueueException {
        if (messageBasedDispatch) {
            for (Iterator iter = queueMap.entrySet().iterator(); iter.hasNext(); ) {
                Entry entry = (Entry) ((Map.Entry) iter.next()).getValue();
                entry.abstractQueue.rollback(entry.transaction, b);
            }
        } else {
            if (baseQueue == null)
                return;
            baseQueue.rollback(baseTx, b);
        }
    }

    public void rollback(boolean b, AsyncCompletionCallback callback) {
        if (messageBasedDispatch) {
            if (queueMap.size() == 0) {
                callback.notifyCallbackStack(true);
                return;
            }
            final RollbackJoin join = new RollbackJoin(callback);
            for (Iterator iter = queueMap.entrySet().iterator(); iter.hasNext(); ) {
                Entry entry = (Entry) ((Map.Entry) iter.next()).getValue();
                join.incNumberCallbacks();
                entry.abstractQueue.rollback(entry.transaction, b, new RollbackCallback(join));
            }
            join.setBlocked(false);
        } else {
            if (baseQueue == null)
                return;
            baseQueue.commit(baseTx, callback);
        }
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[ClusteredTransactionId");
        sb.append(", baseQueue=").append(baseQueue);
        sb.append(", messageBasedDispatch=").append(messageBasedDispatch);
        sb.append(", queueMap=").append(queueMap);
        sb.append(']');
        return sb.toString();
    }

    private class Entry {
        AbstractQueue abstractQueue = null;
        QueueImpl destination = null;
        Object transaction = null;

        private Entry(AbstractQueue abstractQueue, QueueImpl destination, Object transaction) {
            this.abstractQueue = abstractQueue;
            this.destination = destination;
            this.transaction = transaction;
        }
    }

    private class DelayCollector extends CallbackJoin {
        long delay = 0;

        protected DelayCollector(AsyncCompletionCallback asyncCompletionCallback) {
            super(asyncCompletionCallback);
        }

        protected void callbackDone(AsyncCompletionCallback callback, boolean success, boolean last) {
            if (success) {
                Long res = (Long) callback.getResult();
                if (res != null)
                    delay = Math.max(delay, res.longValue());
                if (last) {
                    finalResult = Long.valueOf(delay);
                }
            } else {
                finalSuccess = false;
                finalException = callback.getException();
            }
        }
    }

    private class RollbackJoin extends CallbackJoin {
        protected RollbackJoin(AsyncCompletionCallback asyncCompletionCallback) {
            super(asyncCompletionCallback);
        }

        protected void callbackDone(AsyncCompletionCallback callback, boolean success, boolean last) {
            if (!success) {
                finalSuccess = false;
                finalException = callback.getException();
            }
        }
    }

    private class RollbackCallback extends AsyncCompletionCallback {
        RollbackJoin join = null;

        private RollbackCallback(RollbackJoin join) {
            this.join = join;
        }

        public void done(boolean success) {
            join.done(this, success);
        }
    }
}
