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

package com.swiftmq.impl.xa.standard;

import com.swiftmq.jms.XidImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityAddException;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.QueueTransaction;
import com.swiftmq.swiftlet.xa.XAContextException;

import javax.transaction.xa.XAException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class XARecoveryContextImpl extends XAContextImpl {
    List<Object[]> transactions = new CopyOnWriteArrayList<>();
    final AtomicBoolean closed = new AtomicBoolean(false);
    // This context is necessary for HA recovery
    XALiveContextImpl liveContext = null;

    public XARecoveryContextImpl(SwiftletContext ctx, XidImpl xid) {
        super(ctx, xid);
    }

    public void setPrepared(boolean b) {
    }

    public boolean isPrepared() {
        return liveContext == null || liveContext.isPrepared();
    }

    public int register(String description) throws XAContextException {
        if (liveContext == null)
            liveContext = new XALiveContextImpl(ctx, xid, false);
        return liveContext.register(description);
    }

    public void unregister(int id, boolean rollbackOnly) throws XAContextException {
        liveContext.unregister(id, rollbackOnly);
    }

    void _addTransaction(AbstractQueue queue, Object transactionId) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/_addTransaction, queue=" + queue + ", transactionId: " + transactionId);
        try {
            if (transactions.size() == 0) {
                Entity entity = ctx.preparedUsageList.createEntity();
                entity.setName(Integer.toString(incCount()));
                entity.setDynamicObject(xid);
                entity.createCommands();
                ctx.preparedUsageList.addEntity(entity);
                try {
                    entity.getProperty("xid").setValue(signature);
                } catch (Exception e) {
                }
                EntityList queues = (EntityList) entity.getEntity("queues");
                Entity queueEntity = queues.createEntity();
                queueEntity.setName(queue.getQueueName());
                queueEntity.createCommands();
                queues.addEntity(queueEntity);
            } else {
                Map entities = ctx.preparedUsageList.getEntities();
                for (Object o : entities.entrySet()) {
                    Entity xidEntity = (Entity) ((Map.Entry<?, ?>) o).getValue();
                    EntityList queueList = (EntityList) xidEntity.getEntity("queues");
                    if ((xidEntity.getProperty("xid").getValue()).equals(signature) && queueList.getEntity(queue.getQueueName()) == null) {
                        Entity queueEntity = queueList.createEntity();
                        queueEntity.setName(queue.getQueueName());
                        queueEntity.createCommands();
                        queueList.addEntity(queueEntity);
                        break;
                    }
                }
            }
        } catch (EntityAddException ignored) {
        }
        transactions.add(new Object[]{queue, transactionId});
    }

    public void addTransaction(int id, String queueName, QueueTransaction queueTransaction) throws XAContextException {
        if (liveContext == null)
            throw new XAContextException(XAException.XAER_PROTO, "Operation is not supported on a XARecoveryContextImpl");
        liveContext.addTransaction(id, queueName, queueTransaction);
    }

    public void prepare() throws XAContextException {
        if (liveContext == null)
            throw new XAContextException(XAException.XAER_PROTO, "Operation is not supported on a XARecoveryContextImpl");
        liveContext.prepare();
    }

    public long commit(boolean onePhase) throws XAContextException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/commit onePhase=" + onePhase + " ...");
        if (closed.get())
            throw new XAContextException(XAException.XAER_PROTO, "XA transaction is in closed state");
        if (onePhase)
            throw new XAContextException(XAException.XAER_PROTO, "Operation is not supported on a XARecoveryContextImpl");
        for (Object[] transaction : transactions) {
            Object[] wrapper = transaction;
            try {
                ((AbstractQueue) wrapper[0]).commit(wrapper[1], xid);
                ctx.logSwiftlet.logInformation(ctx.xaSwiftlet.getName(), toString() + "commit xid=" + signature);
            } catch (Exception e) {
                if (!ctx.queueManager.isTemporaryQueue(((AbstractQueue) wrapper[0]).getQueueName()))
                    ctx.logSwiftlet.logError(ctx.xaSwiftlet.getName(), toString() + "commit (two phase) xid=" + signature + ", failed for queue: " + ((AbstractQueue) wrapper[0]).getQueueName());
            }
        }
        if (liveContext != null)
            liveContext.commit(onePhase);
        removeUsageEntity();
        close();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/commit onePhase=" + onePhase + " done");
        return 0;
    }

    public void rollback() throws XAContextException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/rollback...");
        if (closed.get())
            throw new XAContextException(XAException.XAER_PROTO, "XA transaction is in closed state");
        for (Object[] transaction : transactions) {
            Object[] wrapper = (Object[]) transaction;
            try {
                ((AbstractQueue) wrapper[0]).rollback(wrapper[1], xid, true);
                ctx.logSwiftlet.logInformation(ctx.xaSwiftlet.getName(), toString() + "rollback xid=" + signature);
            } catch (Exception e) {
                if (!ctx.queueManager.isTemporaryQueue(((AbstractQueue) wrapper[0]).getQueueName()))
                    ctx.logSwiftlet.logError(ctx.xaSwiftlet.getName(), toString() + "rollback (two phase) xid=" + signature + ", failed for queue: " + ((AbstractQueue) wrapper[0]).getQueueName());
            }
        }
        if (liveContext != null)
            liveContext.rollback();
        removeUsageEntity();
        close();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/rollback done");
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/close...");
        if (closed.get())
            return;
        if (liveContext != null)
            liveContext.close();
        closed.set(true);
        transactions.clear();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/close done");
    }

    public String toString() {
        return "[XARecoveryContextImpl, xid=" + signature + "]";
    }
}
