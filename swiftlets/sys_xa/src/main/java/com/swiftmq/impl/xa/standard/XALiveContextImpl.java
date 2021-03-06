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
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.swiftlet.queue.QueueTransaction;
import com.swiftmq.swiftlet.xa.XAContextException;
import com.swiftmq.tools.collection.ArrayListTool;

import javax.transaction.xa.XAException;
import java.util.ArrayList;
import java.util.List;

public class XALiveContextImpl extends XAContextImpl {
    boolean prepared = false;
    boolean rollbackOnly = false;
    boolean rolledBack = false;
    boolean closed = false;
    boolean wasTimeout = false;
    List transactions = new ArrayList();
    List recoveryTransactions = new ArrayList();
    ArrayList registrations = new ArrayList();
    int nReg = 0;
    boolean registeredUsageList = false;
    long creationTime = 0;

    public XALiveContextImpl(SwiftletContext ctx, XidImpl xid, boolean prepared) {
        super(ctx, xid);
        this.prepared = prepared;
        creationTime = System.currentTimeMillis();
    }

    public void setPrepared(boolean prepared) {
        this.prepared = prepared;
    }

    public boolean isPrepared() {
        return prepared;
    }

    public synchronized int register(String description) throws XAContextException {
        if (prepared)
            throw new XAContextException(XAException.XAER_PROTO, "XA transaction is in prepared state");
        if (rollbackOnly)
            throw new XAContextException(XAException.XA_RBROLLBACK, "XA transaction is marked as rollback-only");
        if (wasTimeout)
            throw new XAContextException(XAException.XA_RBTIMEOUT, "transaction timeout occured");
        if (rolledBack)
            throw new XAContextException(XAException.XA_RBROLLBACK, "XA transaction was rolled back from another thread");
        if (closed)
            throw new XAContextException(XAException.XAER_PROTO, "XA transaction is in closed state");
        nReg++;
        int id = ArrayListTool.setFirstFreeOrExpand(registrations, description);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/register, id=" + id + ", description: " + description);
        return id;
    }

    synchronized void _addTransaction(AbstractQueue queue, Object transactionId) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/_addTransaction, queue=" + queue + ", transactionId: " + transactionId);
        recoveryTransactions.add(new Object[]{queue, transactionId});
    }

    public synchronized void addTransaction(int id, String queueName, QueueTransaction queueTransaction) throws XAContextException {
        if (prepared)
            throw new XAContextException(XAException.XAER_PROTO, "XA transaction is in prepared state");
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/addTransaction, id=" + id + ", queue=" + queueName + ", queueTransaction: " + queueTransaction);
        transactions.add(queueTransaction);
    }

    public synchronized void unregister(int id, boolean rollbackOnly) throws XAContextException {
        if (registrations.get(id) == null)
            throw new XAContextException(XAException.XAER_PROTO, "try to unregister an invalid id");
        nReg--;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/unregister, id=" + id + ", description: " + registrations.get(id));
        registrations.set(id, null);
        this.rollbackOnly = rollbackOnly;
        if (rolledBack) {
            _rollback(false);
            transactions.clear();
            recoveryTransactions.clear();
            if (wasTimeout)
                throw new XAContextException(XAException.XA_RBTIMEOUT, "transaction timeout occured");
            throw new XAContextException(XAException.XA_RBROLLBACK, "XA transaction was rolled back from another thread");
        }
        if (closed)
            throw new XAContextException(XAException.XAER_PROTO, "XA transaction is in closed state");
    }

    // Will be called from a Timer
    synchronized void registerUsageList() {
        if (registeredUsageList || closed || !prepared)
            return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/registerUsageList...");
        Entity entity = ctx.preparedUsageList.createEntity();
        entity.setName(Integer.toString(incCount()));
        entity.setDynamicObject(xid);
        entity.createCommands();
        try {
            ctx.preparedUsageList.addEntity(entity);
            entity.getProperty("xid").setValue(signature);
        } catch (Exception e) {
        }
        registeredUsageList = true;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/registerUsageList done");
    }

    public synchronized void prepare() throws XAContextException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/prepare...");
        if (rollbackOnly)
            throw new XAContextException(XAException.XA_RBROLLBACK, "can't prepare XA transaction because it is set to 'rollback-only'");
        if (wasTimeout)
            throw new XAContextException(XAException.XA_RBTIMEOUT, "transaction timeout occured");
        if (prepared)
            throw new XAContextException(XAException.XAER_PROTO, "XA transaction is already in prepared state");
        if (closed)
            throw new XAContextException(XAException.XAER_PROTO, "XA transaction is in closed state");
        if (nReg > 0)
            throw new XAContextException(XAException.XAER_PROTO, "can't prepare XA transaction because there are still " + nReg + " associations with it");

        // Recovery Transactions are already prepared!
        // Only need to prepare the live tx...
        for (int i = 0; i < transactions.size(); i++) {
            QueueTransaction t = (QueueTransaction) transactions.get(i);
            try {
                t.prepare(xid);
            } catch (Exception e) {
                if (!ctx.queueManager.isTemporaryQueue(t.getQueueName()))
                    ctx.logSwiftlet.logError(ctx.xaSwiftlet.getName(), toString() + "prepare xid=" + signature + ", failed for queue: " + t.getQueueName());
            }
        }
        prepared = true;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/prepare done");
    }

    public synchronized long commit(boolean onePhase) throws XAContextException {
        long fcDelay = 0;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/commit onePhase=" + onePhase + " ...");
        if (wasTimeout)
            throw new XAContextException(XAException.XA_RBTIMEOUT, "transaction timeout occured");
        if (closed)
            throw new XAContextException(XAException.XAER_PROTO, "XA transaction is in closed state");
        if (rollbackOnly)
            throw new XAContextException(XAException.XA_RBROLLBACK, "XA transaction is marked as rollback-only");
        for (int i = 0; i < recoveryTransactions.size(); i++) {
            Object[] wrapper = (Object[]) recoveryTransactions.get(i);
            try {
                ((AbstractQueue) wrapper[0]).commit(wrapper[1], xid);
                ctx.logSwiftlet.logInformation(ctx.xaSwiftlet.getName(), toString() + "commit xid=" + signature);
            } catch (Exception e) {
                if (!ctx.queueManager.isTemporaryQueue(((AbstractQueue) wrapper[0]).getQueueName()))
                    ctx.logSwiftlet.logError(ctx.xaSwiftlet.getName(), toString() + "commit (two phase) xid=" + signature + ", failed for queue: " + ((AbstractQueue) wrapper[0]).getQueueName());
            }
        }
        if (onePhase) {
            if (prepared)
                throw new XAContextException(XAException.XAER_PROTO, "can't use one phase commit, XA transaction is in prepared state");
            for (int i = 0; i < transactions.size(); i++) {
                QueueTransaction t = (QueueTransaction) transactions.get(i);
                try {
                    t.commit();
                    if (t instanceof QueuePushTransaction)
                        fcDelay = Math.max(fcDelay, ((QueuePushTransaction) t).getFlowControlDelay());
                } catch (Exception e) {
                    if (!ctx.queueManager.isTemporaryQueue(t.getQueueName()))
                        ctx.logSwiftlet.logError(ctx.xaSwiftlet.getName(), toString() + "commit (one phase) xid=" + signature + ", failed for queue: " + t.getQueueName());
                }
            }
        } else {
            if (!prepared)
                throw new XAContextException(XAException.XAER_PROTO, "can't use two phase commit, XA transaction is not in prepared state");
            for (int i = 0; i < transactions.size(); i++) {
                QueueTransaction t = (QueueTransaction) transactions.get(i);
                try {
                    t.commit(xid);
                    if (t instanceof QueuePushTransaction)
                        fcDelay = Math.max(fcDelay, ((QueuePushTransaction) t).getFlowControlDelay());
                } catch (Exception e) {
                    if (!ctx.queueManager.isTemporaryQueue(t.getQueueName()))
                        ctx.logSwiftlet.logError(ctx.xaSwiftlet.getName(), toString() + "commit (two phase) xid=" + signature + ", failed for queue: " + t.getQueueName() + ", exception: " + e);
                }
            }
            if (registeredUsageList)
                removeUsageEntity();
        }
        closed = true;
        transactions.clear();
        recoveryTransactions.clear();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/commit onePhase=" + onePhase + " done");
        return fcDelay;
    }

    private void _rollback(boolean reportException) {
        for (int i = 0; i < recoveryTransactions.size(); i++) {
            Object[] wrapper = (Object[]) recoveryTransactions.get(i);
            try {
                ((AbstractQueue) wrapper[0]).rollback(wrapper[1], xid, true);
                ctx.logSwiftlet.logInformation(ctx.xaSwiftlet.getName(), toString() + "rollback xid=" + signature);
            } catch (Exception e) {
                if (!ctx.queueManager.isTemporaryQueue(((AbstractQueue) wrapper[0]).getQueueName()))
                    ctx.logSwiftlet.logError(ctx.xaSwiftlet.getName(), toString() + "rollback (two phase) xid=" + signature + ", failed for queue: " + ((AbstractQueue) wrapper[0]).getQueueName());
            }
        }
        for (int i = 0; i < transactions.size(); i++) {
            QueueTransaction t = (QueueTransaction) transactions.get(i);
            try {
                if (prepared)
                    t.rollback(xid, true);
                else
                    t.rollback();
            } catch (Exception e) {
                if (reportException && !ctx.queueManager.isTemporaryQueue(t.getQueueName()))
                    ctx.logSwiftlet.logError(ctx.xaSwiftlet.getName(), toString() + "rollback xid=" + signature + ", failed for queue: " + t.getQueueName() + ", exception: " + e);
            }
        }
    }

    public synchronized void rollback() throws XAContextException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/rollback...");
        if (wasTimeout)
            throw new XAContextException(XAException.XA_RBTIMEOUT, "transaction timeout occured");
        if (closed)
            throw new XAContextException(XAException.XAER_PROTO, "XA transaction is in closed state");
        _rollback(true);
        if (registeredUsageList)
            removeUsageEntity();
        closed = true;
        rolledBack = true;
        transactions.clear();
        recoveryTransactions.clear();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/rollback done");
    }

    synchronized boolean timeout(long timeoutTime) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/timeout...");
        if (xid.isRouting() || prepared || closed || creationTime > timeoutTime)
            return false;
        _rollback(false);
        if (registeredUsageList)
            removeUsageEntity();
        closed = true;
        rolledBack = true;
        wasTimeout = true;
        transactions.clear();
        recoveryTransactions.clear();
        ctx.logSwiftlet.logWarning(ctx.xaSwiftlet.getName(), toString() + "transaction timeout, transaction rolled back!");
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/timeout done");
        return true;
    }

    public synchronized void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/close...");
        if (closed)
            return;
        closed = true;
        if (!prepared)
            _rollback(false);
        if (registeredUsageList)
            removeUsageEntity();
        closed = true;
        transactions.clear();
        recoveryTransactions.clear();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/close done");
    }

    public String toString() {
        return "[XALiveContextImpl, xid=" + signature + ", prepared=" + prepared + "]";
    }
}
