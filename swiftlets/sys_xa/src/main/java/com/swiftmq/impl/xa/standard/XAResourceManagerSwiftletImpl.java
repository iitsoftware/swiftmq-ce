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
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.QueueTransaction;
import com.swiftmq.swiftlet.store.PrepareLogRecord;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.swiftlet.xa.XAContext;
import com.swiftmq.swiftlet.xa.XAContextException;
import com.swiftmq.swiftlet.xa.XAResourceManagerSwiftlet;
import com.swiftmq.swiftlet.xa.XidFilter;

import java.util.*;

public class XAResourceManagerSwiftletImpl extends XAResourceManagerSwiftlet implements TimerListener {
    SwiftletContext ctx = null;
    Map contexts = new HashMap();
    long scanInterval = 0;
    long defaultTxTimeout = 0;
    long txTimeout = 0;
    TxTimer txTimer = new TxTimer();

    /* @deprecated these are deprecated methods to ensure backward compatibility with SwiftMQ 4.x */
    public synchronized void addPreparedTransaction(XidImpl xid, String queueName, QueueTransaction queueTransaction) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "addPreparedTransaction, xid=" + xid + ", queueName=" + queueName + ", queueTransaction=" + queueTransaction + " ...");
        XAContext xac = (XAContext) contexts.get(xid);
        if (xac == null) {
            xac = new XALiveContextImpl(ctx, xid, true);
            contexts.put(xid, xac);
        }
        try {
            xac.setPrepared(false);
            int id = xac.register(toString());
            xac.addTransaction(id, queueName, queueTransaction);
            xac.unregister(id, false);
            xac.setPrepared(true);
        } catch (XAContextException e) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "addPreparedTransaction, exception: " + e);
            ctx.logSwiftlet.logError(getName(), "addPreparedTransaction, xid=" + xid + ", exception: " + e);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "addPreparedTransaction, xid=" + xid + ", queueName=" + queueName + ", queueTransaction=" + queueTransaction + " done");
    }

    public synchronized void commit(XidImpl xid) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "commit, xid=" + xid + " ...");
        XAContext xac = (XAContext) contexts.get(xid);
        if (xac != null) {
            try {
                xac.commit(false);
            } catch (XAContextException e) {
                if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "commit, exception: " + e);
                ctx.logSwiftlet.logError(getName(), "commit, xid=" + xid + ", exception: " + e);
            }
            removeXAContext(xid);
        } else {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "commit, xid not found: " + xid);
            ctx.logSwiftlet.logWarning(getName(), "commit, xid not found: " + xid);
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "commit, xid=" + xid + " done");
    }

    public synchronized void rollback(XidImpl xid) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "rollback, xid=" + xid + " ...");
        XAContext xac = (XAContext) contexts.get(xid);
        if (xac != null) {
            try {
                xac.rollback();
            } catch (XAContextException e) {
                if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "rollback, exception: " + e);
                ctx.logSwiftlet.logError(getName(), "rollback, xid=" + xid + ", exception: " + e);
            }
            removeXAContext(xid);
        } else {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "rollback, xid not found: " + xid);
            ctx.logSwiftlet.logWarning(getName(), "rollback, xid not found: " + xid);
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "rollback, xid=" + xid + " done");
    }
    /* @deprecated end */

    public void performTimeAction() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "performTimeAction ...");
        Map cloned = null;
        synchronized (this) {
            cloned = (Map) ((HashMap) contexts).clone();
        }
        for (Iterator iter = cloned.entrySet().iterator(); iter.hasNext(); ) {
            XAContext xac = (XAContext) ((Map.Entry) iter.next()).getValue();
            if (xac.isPrepared() && xac instanceof XALiveContextImpl) {
                if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "performTimeAction, register: " + xac);
                ((XALiveContextImpl) xac).registerUsageList();
            }
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "performTimeAction done");
    }

    public synchronized void setTransactionTimeout(long timeout) {
        if (txTimeout != timeout) {
            long to = getTransactionTimeout();
            if (to > 0)
                ctx.timerSwiftlet.removeTimerListener(txTimer);
            txTimeout = timeout;
            to = getTransactionTimeout();
            if (to > 0)
                ctx.timerSwiftlet.addTimerListener(to, txTimer);
        }
    }

    public synchronized long getTransactionTimeout() {
        return txTimeout <= 0 ? defaultTxTimeout : txTimeout;
    }

    public synchronized boolean isHeuristicCompleted(XidImpl xid) {
        return ctx.heuristicHandler.hasHeuristic(xid);
    }

    public synchronized boolean isHeuristicCommit(XidImpl xid) {
        return ctx.heuristicHandler.wasCommit(xid);
    }

    public synchronized boolean isHeuristicRollback(XidImpl xid) {
        return !ctx.heuristicHandler.wasCommit(xid);
    }

    public synchronized List getHeuristicCompletedXids() {
        return ctx.heuristicHandler.getXids();
    }

    public synchronized void forget(XidImpl xid) {
        try {
            ctx.heuristicHandler.removeHeuristic(xid);
        } catch (Exception e) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "forget, exception: " + e);
            ctx.logSwiftlet.logError(getName(), "forget, xid=" + xid + ", exception: " + e);
        }
    }

    public synchronized boolean hasPreparedXid(XidImpl xid) {
        XAContext xac = (XAContext) contexts.get(xid);
        return xac != null && xac.isPrepared();
    }

    public synchronized List getPreparedXids() {
        if (contexts.size() == 0)
            return null;
        List list = new ArrayList();
        for (Iterator iter = contexts.entrySet().iterator(); iter.hasNext(); ) {
            XAContext xac = (XAContext) ((Map.Entry) iter.next()).getValue();
            if (xac.isPrepared())
                list.add(xac.getXid());
        }
        return list.size() == 0 ? null : list;
    }

    public synchronized List getPreparedXids(XidFilter filter) {
        if (contexts.size() == 0)
            return null;
        List list = new ArrayList();
        for (Iterator iter = contexts.entrySet().iterator(); iter.hasNext(); ) {
            XAContext xac = (XAContext) ((Map.Entry) iter.next()).getValue();
            if (xac.isPrepared() && filter.isMatch(xac.getXid()))
                list.add(xac.getXid());
        }
        return list.size() == 0 ? null : list;
    }

    public synchronized XAContext createXAContext(XidImpl xid) {
        XAContext xac = (XAContext) contexts.get(xid);
        if (xac != null)
            return null;
        xac = new XALiveContextImpl(ctx, xid, false);
        contexts.put(xid, xac);
        return xac;
    }

    public synchronized XAContext getXAContext(XidImpl xid) {
        return (XAContext) contexts.get(xid);
    }

    public synchronized void removeXAContext(XidImpl xid) {
        XAContext xac = (XAContext) contexts.remove(xid);
        if (xac != null)
            xac.close();
    }

    private AbstractQueue getPreparedQueue(String queueName) throws Exception {
        AbstractQueue queue = ctx.queueManager.getQueueForInternalUse(queueName);
        if (queue == null) {
            if (!ctx.queueManager.isTemporaryQueue(queueName) && !queueName.startsWith("tpc$")) // hack!
            {
                ctx.queueManager.createQueue(queueName, (ActiveLogin) null);
                queue = ctx.queueManager.getQueueForInternalUse(queueName);
            }
        }
        return queue;
    }

    private void buildPreparedTransactions() throws Exception {
        List prepareRecordList = ctx.storeSwiftlet.getPrepareLogRecords();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "buildPreparedTransactions, recordList: " + prepareRecordList);
        if (prepareRecordList != null && prepareRecordList.size() > 0) {
            for (int i = 0; i < prepareRecordList.size(); i++) {
                PrepareLogRecord record = (PrepareLogRecord) prepareRecordList.get(i);
                AbstractQueue queue = getPreparedQueue(record.getQueueName());
                if (queue != null) {
                    XidImpl xid = record.getGlobalTxId();
                    Object localTxId = queue.buildPreparedTransaction(record);
                    XALiveContextImpl xac = (XALiveContextImpl) contexts.get(xid);
                    if (xac == null) {
                        xac = new XALiveContextImpl(ctx, xid, true);
                        xac.setRecovered(true);
                        xac.registerUsageList();
                        contexts.put(xid, xac);
                    }
                    xac._addTransaction(queue, localTxId);
                }
            }
        }
    }

    protected void startup(Configuration configuration) throws SwiftletException {
        ctx = new SwiftletContext(this, configuration);

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup...");

        try {
            ctx.heuristicHandler.loadHeuristics();
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "exception during loadHeuristics: " + e.toString());
            ctx.logSwiftlet.logError(getName(), "exception during loadHeuristics: " + e.toString());
        }

        try {
            buildPreparedTransactions();
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "exception during buildPreparedTransactions: " + e.toString());
            ctx.logSwiftlet.logError(getName(), "exception during buildPreparedTransactions: " + e.toString());
        }

        if (contexts.size() > 0) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), contexts.size() + " prepared transactions found");
            ctx.logSwiftlet.logWarning(getName(), contexts.size() + " prepared transactions found!");
            System.out.println("+++ WARNING! " + contexts.size() + " prepared transactions found!");
            System.out.println("+++          HA/Routing XA transactions are automatically recovered.");
            System.out.println("+++          You may also use Explorer/CLI for heuristic commit or rollback.");
        }

        Property prop = configuration.getProperty("scan-interval");
        scanInterval = ((Long) prop.getValue()).longValue();
        ctx.timerSwiftlet.addTimerListener(scanInterval, this);
        prop.setPropertyChangeListener(new PropertyChangeListener() {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                ctx.timerSwiftlet.removeTimerListener(XAResourceManagerSwiftletImpl.this);
                scanInterval = ((Long) newValue).longValue();
                ctx.timerSwiftlet.addTimerListener(scanInterval, XAResourceManagerSwiftletImpl.this);
            }
        });
        prop = configuration.getProperty("default-transaction-timeout");
        defaultTxTimeout = ((Long) prop.getValue()).longValue();
        long timeout = getTransactionTimeout();
        if (timeout > 0)
            ctx.timerSwiftlet.addTimerListener(timeout, txTimer);
        prop.setPropertyChangeListener(new PropertyChangeListener() {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                long timeout = getTransactionTimeout();
                if (timeout > 0)
                    ctx.timerSwiftlet.removeTimerListener(txTimer);
                defaultTxTimeout = ((Long) newValue).longValue();
                timeout = getTransactionTimeout();
                if (timeout > 0)
                    ctx.timerSwiftlet.addTimerListener(timeout, txTimer);
            }
        });

        CommandRegistry commandRegistry = ctx.preparedUsageList.getCommandRegistry();
        CommandExecutor commitExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length != 2)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'commit <id>'"};
                Entity e = ctx.preparedUsageList.getEntity(cmd[1]);
                if (e == null)
                    return new String[]{TreeCommands.ERROR, "Unknown Entity: " + cmd[1]};
                XAContext xac = (XAContext) contexts.get(e.getDynamicObject());
                XidImpl xid = xac.getXid();
                try {
                    xac.commit(false);
                    if (!xid.isRouting())
                        ctx.heuristicHandler.addHeuristic(xid, true);
                } catch (Exception e1) {
                    return new String[]{TreeCommands.ERROR, "Exception during commit: " + e1};
                }
                removeXAContext(xid);
                return null;
            }
        };
        Command commitCommand = new Command("commit", "commit <id>", "Commit", true, commitExecutor, true, true);
        commandRegistry.addCommand(commitCommand);
        CommandExecutor rollbackExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length != 2)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'rollback <id>'"};
                Entity e = ctx.preparedUsageList.getEntity(cmd[1]);
                if (e == null)
                    return new String[]{TreeCommands.ERROR, "Unknown Entity: " + cmd[1]};
                XAContext xac = (XAContext) contexts.get(e.getDynamicObject());
                XidImpl xid = xac.getXid();
                try {
                    xac.rollback();
                    if (!xid.isRouting())
                        ctx.heuristicHandler.addHeuristic(xid, false);
                } catch (Exception e1) {
                    return new String[]{TreeCommands.ERROR, "Exception during rollback: " + e1};
                }
                removeXAContext(xid);
                return null;
            }
        };
        Command rollbackCommand = new Command("rollback", "rollback <id>", "Rollback", true, rollbackExecutor, true, true);
        commandRegistry.addCommand(rollbackCommand);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup...done");
    }

    protected void shutdown() throws SwiftletException {
        // true when shutdown while standby
        if (ctx == null)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown...");
        ctx.heuristicHandler.close();
        ctx.timerSwiftlet.removeTimerListener(this);
        if (defaultTxTimeout > 0)
            ctx.timerSwiftlet.removeTimerListener(txTimer);
        for (Iterator iter = contexts.entrySet().iterator(); iter.hasNext(); ) {
            XAContext xac = (XAContext) ((Map.Entry) iter.next()).getValue();
            xac.close();
        }
        contexts.clear();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown...done");
        ctx = null;
    }

    private class TxTimer implements TimerListener {
        public void performTimeAction() {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "TxTimer/performTimeAction ...");
            long timeoutTime = System.currentTimeMillis() - getTransactionTimeout();
            Map cloned = null;
            synchronized (XAResourceManagerSwiftletImpl.this) {
                cloned = (Map) ((HashMap) contexts).clone();
            }
            for (Iterator iter = cloned.entrySet().iterator(); iter.hasNext(); ) {
                XAContext xac = (XAContext) ((Map.Entry) iter.next()).getValue();
                if (xac instanceof XALiveContextImpl) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "TxTimer/performTimeAction, checking: " + xac);
                    if (((XALiveContextImpl) xac).timeout(timeoutTime))
                        removeXAContext(xac.getXid());
                }
            }
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "TxTimer/performTimeAction done");
        }
    }
}
