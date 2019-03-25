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

package com.swiftmq.impl.queue.standard.composite;

import com.swiftmq.impl.queue.standard.SwiftletContext;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.jms.XidImpl;
import com.swiftmq.mgmt.*;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.FlowController;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.store.CompositeStoreTransaction;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;
import com.swiftmq.tools.concurrent.CallbackJoin;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.util.IdGenerator;

import javax.jms.InvalidSelectorException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CompositeQueue extends AbstractQueue {
    SwiftletContext ctx = null;
    Entity compositeQueueEntity = null;
    EntityListEventAdapter queueBindingsAdapter = null;
    EntityListEventAdapter topicBindingsAdapter = null;
    EntityList queueBindings = null;
    EntityList topicBindings = null;
    String idPrefix = null;
    long sequenceNo = 0;

    public CompositeQueue(SwiftletContext ctx, Entity compositeQueueEntity) {
        this.ctx = ctx;
        this.compositeQueueEntity = compositeQueueEntity;
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "created");
    }

    private void init() {
        try {
            StringBuffer b = new StringBuffer(InetAddress.getLocalHost().getHostName());
            b.append('/');
            b.append(IdGenerator.getInstance().nextId('/'));
            b.append('/');
            idPrefix = b.toString();
        } catch (UnknownHostException e) {
        }
        queueBindings = (EntityList) compositeQueueEntity.getEntity("queue-bindings");
        queueBindingsAdapter = new EntityListEventAdapter((EntityList) compositeQueueEntity.getEntity("queue-bindings"), true, false) {
            public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException {
                try {
                    if (ctx.queueSpace.enabled)
                        ctx.queueSpace.trace(getQueueName(), "onEntityAdd (Queue Binding): " + newEntity.getName() + " ...");
                    Property prop = newEntity.getProperty("message-selector");
                    String selector = (String) prop.getValue();
                    if (selector != null && selector.trim().length() > 0) {
                        MessageSelector ms = new MessageSelector(selector);
                        ms.compile();
                        newEntity.setUserObject(ms);
                    }
                    prop.setPropertyChangeListener(new MSChangeListener(newEntity));
                    if (ctx.queueSpace.enabled)
                        ctx.queueSpace.trace(getQueueName(), "onEntityAdd (Queue Binding): " + newEntity.getName() + " done.");
                } catch (Exception e) {
                    if (ctx.queueSpace.enabled)
                        ctx.queueSpace.trace(getQueueName(), "onEntityAdd (Queue Binding), exception=" + e);
                    throw new EntityAddException(e.getMessage());
                }
            }
        };
        try {
            queueBindingsAdapter.init();
        } catch (Exception e) {
        }
        topicBindings = (EntityList) compositeQueueEntity.getEntity("topic-bindings");
        topicBindingsAdapter = new EntityListEventAdapter((EntityList) compositeQueueEntity.getEntity("topic-bindings"), true, false) {
            public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException {
                try {
                    if (ctx.queueSpace.enabled)
                        ctx.queueSpace.trace(getQueueName(), "onEntityAdd (Topic Binding): " + newEntity.getName() + " ...");
                    Property prop = newEntity.getProperty("message-selector");
                    String selector = (String) prop.getValue();
                    if (selector != null && selector.trim().length() > 0) {
                        MessageSelector ms = new MessageSelector(selector);
                        ms.compile();
                        newEntity.setUserObject(ms);
                    }
                    prop.setPropertyChangeListener(new MSChangeListener(newEntity));
                    if (ctx.queueSpace.enabled)
                        ctx.queueSpace.trace(getQueueName(), "onEntityAdd (Topic Binding): " + newEntity.getName() + " done.");
                } catch (Exception e) {
                    if (ctx.queueSpace.enabled)
                        ctx.queueSpace.trace(getQueueName(), "onEntityAdd (Topic Binding), exception=" + e);
                    throw new EntityAddException(e.getMessage());
                }
            }
        };
        try {
            topicBindingsAdapter.init();
        } catch (Exception e) {
        }
    }

    private MessageImpl copyMessage(MessageImpl msg) throws Exception {
        DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();
        DataByteArrayInputStream dbis = new DataByteArrayInputStream();
        msg.writeContent(dbos);
        dbis.reset();
        dbis.setBuffer(dbos.getBuffer(), 0, dbos.getCount());
        MessageImpl msgCopy = MessageImpl.createInstance(dbis.readInt());
        msgCopy.readContent(dbis);
        return msgCopy;
    }

    private synchronized String nextId() {
        StringBuffer b = new StringBuffer(idPrefix);
        b.append(sequenceNo++);
        if (sequenceNo == Long.MAX_VALUE)
            sequenceNo = 0;
        return b.toString();
    }

    public void lockQueue(Object object) {
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "lockQueue, cTxId=" + object);
        List entries = ((CompositeTransactionId) object).getEntries();
        for (int i = 0; i < entries.size(); i++) {
            CompositeTransactionIdEntry entry = (CompositeTransactionIdEntry) entries.get(i);
            entry.queue.lockQueue(entry.txId);
        }
    }

    public void unlockQueue(Object object, boolean markAsyncActive) {
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "unlockQueue, cTxId=" + object);
        List entries = ((CompositeTransactionId) object).getEntries();
        for (int i = 0; i < entries.size(); i++) {
            CompositeTransactionIdEntry entry = (CompositeTransactionIdEntry) entries.get(i);
            entry.queue.unlockQueue(entry.txId, markAsyncActive);
        }
    }

    public void setCompositeStoreTransaction(Object object, CompositeStoreTransaction ct) {
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "setCompositeStoreTransaction, cTxId=" + object + ", compositeStoreTx=" + ct);
        ((CompositeTransactionId) object).setCompositeStoreTransaction(ct);
    }

    public boolean hasReceiver(MessageImpl message) {
        return false;
    }

    public Object createPushTransaction() throws QueueException {
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "createPushTransaction ...");
        CompositeTransactionId cTxId = new CompositeTransactionId();
        Map m = queueBindings.getEntities();
        if (m != null && m.size() > 0) {
            for (Iterator iter = m.entrySet().iterator(); iter.hasNext(); ) {
                Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
                AbstractQueue queue = ctx.queueManager.getQueueForInternalUse(entity.getName(), true);
                if (queue == null) {
                    ctx.logSwiftlet.logError(ctx.queueManager.getName(), "Composite Queue '" + getQueueName() + "': Queue Binding, queue '" + entity.getName() + "' not found!");
                    if (ctx.queueSpace.enabled)
                        ctx.queueSpace.trace(getQueueName(), "Queue Binding, queue '" + entity.getName() + "' not found!");
                } else {
                    cTxId.add(new CompositeTransactionIdEntry(true, queue, queue.createPushTransaction(),
                            (MessageSelector) entity.getUserObject(),
                            ((Boolean) entity.getProperty("generate-new-message-id").getValue()).booleanValue(),
                            ((Boolean) entity.getProperty("change-destination").getValue()).booleanValue(),
                            ((Boolean) entity.getProperty("default-delivery").getValue()).booleanValue(),
                            entity.getName()));
                }
            }
        }
        m = topicBindings.getEntities();
        if (m != null && m.size() > 0) {
            for (Iterator iter = m.entrySet().iterator(); iter.hasNext(); ) {
                Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
                String queueName = ctx.topicManager.getQueueForTopic(entity.getName());
                AbstractQueue queue = ctx.queueManager.getQueueForInternalUse(queueName, true);
                if (queue == null) {
                    ctx.logSwiftlet.logError(ctx.queueManager.getName(), "Composite Queue '" + getQueueName() + "': Topic Binding, topic '" + entity.getName() + "' not found!");
                    if (ctx.queueSpace.enabled)
                        ctx.queueSpace.trace(getQueueName(), "Topic Binding, topic '" + entity.getName() + "' not found!");
                } else {
                    cTxId.add(new CompositeTransactionIdEntry(false, queue, queue.createPushTransaction(),
                            (MessageSelector) entity.getUserObject(),
                            ((Boolean) entity.getProperty("generate-new-message-id").getValue()).booleanValue(),
                            false,
                            ((Boolean) entity.getProperty("default-delivery").getValue()).booleanValue(),
                            entity.getName()));
                }
            }
        }
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "createPushTransaction done, cTxId=" + cTxId);
        return cTxId;
    }

    public Object createPullTransaction() throws QueueException {
        throw new QueueException("Operation not supported on a Composite Queue!");
    }

    public void putMessage(Object object, MessageImpl message) throws QueueException {
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "putMessage, cTxId=" + object + " ...");
        boolean needCopy = false;
        boolean hasDefaultDelivery = false;
        int timesDelivered = 0;
        List entries = ((CompositeTransactionId) object).getEntries();
        for (int i = 0; i < entries.size(); i++) {
            CompositeTransactionIdEntry entry = (CompositeTransactionIdEntry) entries.get(i);
            if (entry.isDefaultBinding)
                hasDefaultDelivery = true;
            if (!entry.isDefaultBinding && (entry.selector == null || entry.selector.isSelected(message))) {
                try {
                    MessageImpl m = message;
                    if (needCopy)
                        m = copyMessage(message);
                    else
                        needCopy = true;
                    if (entry.generateNewMessageId)
                        m.setJMSMessageID(nextId());
                    if (entry.changeDestination)
                        m.setJMSDestination(new QueueImpl(ctx.queueManager.fqn(entry.originalName)));
                    if (!entry.isQueue)
                        m.setJMSDestination(new TopicImpl(entry.originalName));
                    if (ctx.queueSpace.enabled)
                        ctx.queueSpace.trace(getQueueName(), "putMessage on binding '" + entry.queue.getQueueName());
                    entry.queue.putMessage(entry.txId, m);
                    timesDelivered++;
                } catch (Exception e) {
                    ctx.logSwiftlet.logError(ctx.queueManager.getName(), "Composite Queue '" + getQueueName() + "': putMessage on binding queue '" + entry.queue.getQueueName() + "', exception=" + e);
                    if (ctx.queueSpace.enabled)
                        ctx.queueSpace.trace(getQueueName(), "putMessage on binding queue '" + entry.queue.getQueueName() + "', exception=" + e);
                }
            }
        }
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "putMessage delivered to " + timesDelivered + " bindings, hasDefaultDelivery=" + hasDefaultDelivery);
        if (timesDelivered == 0 && hasDefaultDelivery) {
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "putMessage, let's check the default bindings ...");
            for (int i = 0; i < entries.size(); i++) {
                CompositeTransactionIdEntry entry = (CompositeTransactionIdEntry) entries.get(i);
                if (entry.isDefaultBinding) {
                    try {
                        MessageImpl m = message;
                        if (needCopy)
                            m = copyMessage(message);
                        else
                            needCopy = true;
                        if (entry.generateNewMessageId)
                            m.setJMSMessageID(nextId());
                        if (entry.changeDestination)
                            m.setJMSDestination(new QueueImpl(ctx.queueManager.fqn(entry.originalName)));
                        if (!entry.isQueue)
                            m.setJMSDestination(new TopicImpl(entry.originalName));
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "putMessage on DEFAULT binding '" + entry.queue.getQueueName());
                        entry.queue.putMessage(entry.txId, m);
                    } catch (Exception e) {
                        ctx.logSwiftlet.logError(ctx.queueManager.getName(), "Composite Queue '" + getQueueName() + "': putMessage on binding queue '" + entry.queue.getQueueName() + "', exception=" + e);
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "putMessage on binding queue '" + entry.queue.getQueueName() + "', exception=" + e);
                    }
                }
            }
        }
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "putMessage, cTxId=" + object + " done");
    }

    public void prepare(Object object, XidImpl xid) throws QueueException {
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "prepare, cTxId=" + object + ", xid=" + xid + " ...");
        List entries = ((CompositeTransactionId) object).getEntries();
        for (int i = 0; i < entries.size(); i++) {
            CompositeTransactionIdEntry entry = (CompositeTransactionIdEntry) entries.get(i);
            entry.queue.prepare(entry.txId, xid);
        }
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "prepare, cTxId=" + object + ", xid=" + xid + " done.");
    }

    public void commit(Object object, XidImpl xid) throws QueueException {
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "commit, cTxId=" + object + ", xid=" + xid + " ...");
        long delay = 0;
        List entries = ((CompositeTransactionId) object).getEntries();
        for (int i = 0; i < entries.size(); i++) {
            CompositeTransactionIdEntry entry = (CompositeTransactionIdEntry) entries.get(i);
            entry.queue.commit(entry.txId, xid);
            FlowController fc = entry.queue.getFlowController();
            if (fc != null)
                delay = Math.max(delay, fc.getNewDelay());
        }
        ((CompositeQueueFlowController) getFlowController()).setLastDelay(delay);
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "commit, cTxId=" + object + ", xid=" + xid + " done.");
    }

    public void commit(Object object) throws QueueException {
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "commit, cTxId=" + object + " ...");
        long delay = 0;
        CompositeTransactionId cTxId = (CompositeTransactionId) object;
        List entries = cTxId.getEntries();
        for (int i = 0; i < entries.size(); i++) {
            CompositeTransactionIdEntry entry = (CompositeTransactionIdEntry) entries.get(i);
            if (cTxId.getCompositeStoreTransaction() != null)
                entry.queue.setCompositeStoreTransaction(entry.txId, cTxId.getCompositeStoreTransaction());
            entry.queue.commit(entry.txId);
            FlowController fc = entry.queue.getFlowController();
            if (fc != null)
                delay = Math.max(delay, fc.getNewDelay());
            if (cTxId.getCompositeStoreTransaction() != null)
                entry.queue.setCompositeStoreTransaction(entry.txId, null);
        }
        ((CompositeQueueFlowController) getFlowController()).setLastDelay(delay);
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "commit, cTxId=" + object + " done.");
    }

    public void commit(Object object, AsyncCompletionCallback callback) {
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "commit (callback), cTxId=" + object + " ...");
        DelayCollector delayCollector = new DelayCollector(callback);
        List entries = ((CompositeTransactionId) object).getEntries();
        for (int i = 0; i < entries.size(); i++) {
            CompositeTransactionIdEntry entry = (CompositeTransactionIdEntry) entries.get(i);
            delayCollector.incNumberCallbacks();
            entry.queue.commit(entry.txId, new CommitCallback(delayCollector, entry.queue));
        }
        delayCollector.setBlocked(false);
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "commit (callback), cTxId=" + object + " done.");
    }

    public void rollback(Object object, XidImpl xid, boolean setRedelivered) throws QueueException {
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "rollback, cTxId=" + object + ", xid=" + xid + " ...");
        List entries = ((CompositeTransactionId) object).getEntries();
        for (int i = 0; i < entries.size(); i++) {
            CompositeTransactionIdEntry entry = (CompositeTransactionIdEntry) entries.get(i);
            entry.queue.rollback(entry.txId, xid, setRedelivered);
        }
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "rollback, cTxId=" + object + ", xid=" + xid + " done.");
    }

    public void rollback(Object object, boolean setRedelivered) throws QueueException {
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "rollback, cTxId=" + object + " ...");
        List entries = ((CompositeTransactionId) object).getEntries();
        for (int i = 0; i < entries.size(); i++) {
            CompositeTransactionIdEntry entry = (CompositeTransactionIdEntry) entries.get(i);
            entry.queue.rollback(entry.txId, setRedelivered);
        }
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "rollback, cTxId=" + object + " done.");
    }

    public void rollback(Object object, boolean setRedelivered, AsyncCompletionCallback callback) {
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "rollback, cTxId=" + object + " ...");
        RollbackJoin join = new RollbackJoin(callback);
        List entries = ((CompositeTransactionId) object).getEntries();
        for (int i = 0; i < entries.size(); i++) {
            CompositeTransactionIdEntry entry = (CompositeTransactionIdEntry) entries.get(i);
            join.incNumberCallbacks();
            entry.queue.rollback(entry.txId, setRedelivered, new RollbackCallback(join));
        }
        join.setBlocked(false);
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "rollback, cTxId=" + object + " done.");
    }

    public void deleteContent() throws QueueException {
    }

    public void startQueue() throws QueueException {
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "startQueue ...");
        init();
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "startQueue done");
    }

    public void stopQueue() throws QueueException {
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "stopQueue ...");
        try {
            queueBindingsAdapter.close();
        } catch (Exception e) {
        }
        try {
            topicBindingsAdapter.close();
        } catch (Exception e) {
        }
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "stopQueue done");
    }

    private class MSChangeListener implements PropertyChangeListener {
        Entity parent = null;

        private MSChangeListener(Entity parent) {
            this.parent = parent;
        }

        public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "propertyChanged (Message Selector): " + parent.getName() + " ...");
            String selector = (String) newValue;
            if (selector != null && selector.trim().length() > 0) {
                MessageSelector ms = new MessageSelector(selector);
                try {
                    ms.compile();
                } catch (InvalidSelectorException e) {
                    throw new PropertyChangeException(e.toString());
                }
                parent.setUserObject(ms);
            } else
                parent.setUserObject(null);
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "propertyChanged (Message Selector): " + parent.getName() + " done.");
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
                if (last)
                    finalResult = Long.valueOf(delay);
            } else {
                finalSuccess = false;
                finalException = callback.getException();
            }
        }
    }

    private class CommitCallback extends AsyncCompletionCallback {
        DelayCollector delayCollector = null;
        AbstractQueue queue = null;

        private CommitCallback(DelayCollector delayCollector, AbstractQueue queue) {
            this.delayCollector = delayCollector;
            this.queue = queue;
        }

        public void done(boolean success) {
            if (queue.getFlowController() != null)
                setResult(Long.valueOf(queue.getFlowController().getLastDelay()));
            else
                setResult(0L);
            delayCollector.done(this, success);
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
