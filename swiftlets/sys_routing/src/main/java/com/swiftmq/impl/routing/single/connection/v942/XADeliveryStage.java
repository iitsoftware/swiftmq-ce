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

package com.swiftmq.impl.routing.single.connection.v942;

import com.swiftmq.impl.routing.single.RoutingSwiftletImpl;
import com.swiftmq.impl.routing.single.SwiftletContext;
import com.swiftmq.impl.routing.single.connection.RoutingConnection;
import com.swiftmq.impl.routing.single.connection.stage.Stage;
import com.swiftmq.impl.routing.single.schedule.SchedulerRegistry;
import com.swiftmq.impl.routing.single.smqpr.*;
import com.swiftmq.impl.routing.single.smqpr.v942.*;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.XidImpl;
import com.swiftmq.swiftlet.queue.QueuePullTransaction;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.swiftlet.queue.QueueReceiver;
import com.swiftmq.swiftlet.queue.QueueSender;
import com.swiftmq.swiftlet.xa.XAContext;
import com.swiftmq.swiftlet.xa.XAContextException;
import com.swiftmq.tools.concurrent.AtomicWrappingCounterInteger;
import com.swiftmq.tools.prop.ParSub;
import com.swiftmq.tools.requestreply.Request;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class XADeliveryStage extends Stage {
    public static final String XID_BRANCH = "swiftmq/src=$0/dest=$1";

    SMQRVisitor visitor = null;
    boolean listener = false;
    String recoveryBranchQ = null;
    byte[] recoveryBranchQB = null;
    String txBase = null;
    AtomicWrappingCounterInteger txNo = new AtomicWrappingCounterInteger(0);
    Map<XidImpl, QueuePullTransaction> outboundTransactions = null;
    Map<XidImpl, Tx> inboundTransactions = null;
    Map<String, QueueSender> producers = null;
    Map<String, QueueReceiver> consumers = null;
    Map<XidImpl, DeliveryRequest> notificationList = null;
    final AtomicBoolean closed = new AtomicBoolean(false);
    ThrottleQueue throttleQueue = null;

    public XADeliveryStage(SwiftletContext ctx, RoutingConnection routingConnection) {
        super(ctx, routingConnection);
        visitor = routingConnection.getVisitor();
        listener = routingConnection.isListener();
        recoveryBranchQ = ParSub.substitute(XID_BRANCH, new String[]{ctx.routerName, routingConnection.getRouterName()});
        recoveryBranchQB = recoveryBranchQ.getBytes();
        outboundTransactions = new HashMap<>();
        inboundTransactions = new HashMap<>();
        notificationList = new HashMap<>();
        producers = new HashMap<>();
        consumers = new HashMap<>();
        if (ctx.inboundFCEnabled)
            throttleQueue = new ThrottleQueue(ctx, routingConnection);

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/created");
        try {
            producers.put(RoutingSwiftletImpl.UNROUTABLE_QUEUE, ctx.queueManager.createQueueSender(RoutingSwiftletImpl.UNROUTABLE_QUEUE, null));
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/Exception creating unroutable sender: " + e);
        }
        txBase = System.currentTimeMillis() + "-";
    }

    private void processTransactionRequest(TransactionRequest request) throws Exception {
        XidImpl xid = request.getXid();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString("INBOUND") + "/processTransactionRequest, xid=" + xid + " ...");
        Tx tx = new Tx(xid);
        inboundTransactions.put(xid, tx);
        List<MessageImpl> messageList = request.getMessageList();
        for (MessageImpl msg : messageList) {
            boolean msgValid = true;
            String queueName;
            if (msg.getDestRouter().equals(ctx.routerName))
                queueName = msg.getDestQueue();
            else
                queueName = SchedulerRegistry.QUEUE_PREFIX + msg.getDestRouter();
            QueueSender sender = producers.get(queueName);
            if (sender == null) {
                try {
                    sender = ctx.queueManager.createQueueSender(queueName, null);
                    producers.put(queueName, sender);
                } catch (Exception e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString("INBOUND") + "/processTransactionRequest, xid=" + xid + ", exception creating sender, queue=" + queueName);
                    if (ctx.queueManager.isTemporaryQueue(queueName)) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString("INBOUND") + "/processTransactionRequest, xid=" + xid + ", temp queue, forget it");
                        msgValid = false;
                    } else {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString("INBOUND") + "/processTransactionRequest, xid=" + xid + ", using unroutable queue");
                        sender = producers.get(RoutingSwiftletImpl.UNROUTABLE_QUEUE);
                        msg.setStringProperty(MessageImpl.PROP_UNROUTABLE_REASON, e.toString());
                    }
                }
            }
            if (msgValid) {
                try {
                    QueuePushTransaction t = tx.getTransaction(queueName);
                    if (t == null) {
                        t = sender.createTransaction();
                        tx.addTransaction(queueName, t);
                    }
                    t.putMessage(msg);
                } catch (Exception e) {
                    try {
                        if (!sender.getQueueName().startsWith(RoutingSwiftletImpl.UNROUTABLE_QUEUE))
                            sender.close();
                    } catch (Exception e1) {
                    }
                    producers.remove(queueName);
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString("INBOUND") + "/processTransactionRequest, xid=" + xid + ", exception put message, queue=" + queueName);
                    if (ctx.queueManager.isTemporaryQueue(queueName)) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString("INBOUND") + "/processTransactionRequest, xid=" + xid + ", temp queue, forget it");
                    } else {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString("INBOUND") + "/processTransactionRequest, xid=" + xid + ", using unroutable queue");
                        sender = producers.get(RoutingSwiftletImpl.UNROUTABLE_QUEUE);
                        msg.setStringProperty(MessageImpl.PROP_UNROUTABLE_REASON, e.toString());
                        QueuePushTransaction t = tx.getTransaction(RoutingSwiftletImpl.UNROUTABLE_QUEUE);
                        if (t == null) {
                            t = sender.createTransaction();
                            tx.addTransaction(RoutingSwiftletImpl.UNROUTABLE_QUEUE, t);
                        }
                        t.putMessage(msg);
                    }
                }
            }
        }
        try {
            tx.prepare();
        } catch (Exception e) {
            tx.rollback();
            inboundTransactions.remove(xid);
            throw e;
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString("INBOUND") + "/processTransactionRequest, xid=" + xid + " done");
    }

    private long commitLocalXid(XidImpl xid) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString("OUTBOUND") + "/processTransactionRequest, xid=" + xid + " ...");
        Tx tx = inboundTransactions.get(xid);
        long delay = tx.commit();
        inboundTransactions.remove(xid);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString("OUTBOUND") + "/processTransactionRequest, xid=" + xid + " done");
        return delay;
    }

    protected void init() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/init...");
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.SMQRFactory.START_STAGE_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + "...");
            int txSize = (Integer) routingConnection.getEntity().getProperty("inbound-transaction-size").getValue();
            int windowSize = (Integer) routingConnection.getEntity().getProperty("inbound-window-size").getValue();
            AdjustRequest rc = new AdjustRequest(txSize, windowSize);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + ", sending request=" + rc);
            routingConnection.getOutboundQueue().enqueue(rc);
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.SMQRFactory.SEND_ROUTE_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + "...");
            RouteRequest rc = new RouteRequest(((SendRouteRequest) request).getRoute());
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + ", sending request=" + rc);
            routingConnection.getOutboundQueue().enqueue(rc);
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.ROUTE_REQ, new RequestHandler() {
            public void visited(Request request) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XADeliveryStage.this.toString("INBOUND") + "/visited, request=" + request + "...");
                RouteRequest rc = (RouteRequest) request;
                try {
                    ctx.routeExchanger.processRoute(routingConnection, rc.getRoute(ctx.routeExchanger.getRouteConverter()));
                } catch (Exception e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XADeliveryStage.this.toString("INBOUND") + "/visited, request=" + request + " exception=" + e);
                    ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), XADeliveryStage.this + "/visited, request=" + request + " exception=" + e);
                }
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.ADJUST_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XADeliveryStage.this + "/visited, request=" + request + "...");
            AdjustRequest rc = (AdjustRequest) request;
            routingConnection.setTransactionSize(rc.getTransactionSize());
            routingConnection.setWindowSize(rc.getWindowSize());
            // A listener must wait until the connector sends a request.
            // It then sends a request by itself to ensure the XADeliveryStage is active at the connector side.
            if (listener)
                getStageQueue().enqueue(new StartStageRequest());
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XADeliveryStage.this + "/visited, request=" + request + ", activating connection");
            routingConnection.getActivationListener().activated(routingConnection);
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.SMQRFactory.DELIVERY_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + "...");
            try {
                DeliveryRequest rc = (DeliveryRequest) request;
                QueuePullTransaction srcTx = rc.readTransaction;
                QueueReceiver receiver = consumers.get(srcTx.getQueueName());
                if (receiver == null) {
                    receiver = ctx.queueManager.createQueueReceiver(srcTx.getQueueName(), null, null);
                    consumers.put(srcTx.getQueueName(), receiver);
                }
                QueuePullTransaction destTx = receiver.createTransaction(false);
                List<MessageImpl> al = new ArrayList<>();
                for (int i = 0; i < rc.len; i++) {
                    destTx.moveToTransaction(rc.entries[i].getMessageIndex(), srcTx);
                    MessageImpl msg = rc.entries[i].getMessage();
                    if (msg.getSourceRouter() == null)
                        msg.setSourceRouter(ctx.routerName);
                    if (msg.getDestRouter() == null)
                        msg.setDestRouter(rc.destinationRouter);
                    al.add(rc.entries[i].getMessage());
                }
                XidImpl xid = new XidImpl(recoveryBranchQB, txNo.get(), (txBase + txNo).getBytes());
                xid.setRouting(true);
                destTx.prepare(xid);
                outboundTransactions.put(xid, destTx);
                TransactionRequest txr = new TransactionRequest(txNo.get(), xid, al);
                txNo.getAndIncrement();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + " sending request=" + txr);
                routingConnection.getOutboundQueue().enqueue(txr);
                if (outboundTransactions.size() <= routingConnection.getWindowSize())
                    rc.callback.delivered(rc);
                else
                    notificationList.put(xid, rc);
            } catch (Exception e) {
                e.printStackTrace();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + " exception=" + e);
                ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), XADeliveryStage.this + "/visited, request=" + request + " exception=" + e + ", disconnecting");
                ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.COMMIT_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + "...");
            CommitRequest rc = (CommitRequest) request;
            XidImpl xid = rc.getXid();
            QueuePullTransaction t = outboundTransactions.remove(xid);
            try {
                t.commit(xid);
                CommitReplyRequest crr = new CommitReplyRequest(xid);
                crr.setOk(true);
                routingConnection.getOutboundQueue().enqueue(crr);
                DeliveryRequest dr = notificationList.remove(xid);
                if (dr != null) {
                    dr.callback.delivered(dr);
                }
            } catch (Exception e) {
                e.printStackTrace();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + " exception=" + e);
                ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), XADeliveryStage.this + "/visited, request=" + request + " exception=" + e + ", disconnecting");
                ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.TRANSACTION_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XADeliveryStage.this.toString("INBOUND") + "/visited, request=" + request + "...");
            TransactionRequest rc = (TransactionRequest) request;
            try {
                processTransactionRequest(rc);
                CommitRequest cr = new CommitRequest(rc.getXid());
                if (throttleQueue != null)
                    throttleQueue.enqueue(cr);
                else
                    routingConnection.getOutboundQueue().enqueue(cr);
            } catch (Exception e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XADeliveryStage.this.toString("INBOUND") + "/visited, request=" + request + " exception=" + e);
                ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), XADeliveryStage.this + "/visited, request=" + request + " exception=" + e + ", disconnecting");
                ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.COMMIT_REPREQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XADeliveryStage.this.toString("INBOUND") + "/visited, request=" + request + "...");
            CommitReplyRequest rc = (CommitReplyRequest) request;
            try {
                if (!rc.isOk())
                    throw new Exception("Reply states not ok: " + rc);
                long delay = commitLocalXid(rc.getXid());
                if (delay > 0 && throttleQueue != null)
                    throttleQueue.enqueue(new ThrottleRequest(delay));
            } catch (Exception e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XADeliveryStage.this.toString("INBOUND") + "/visited, request=" + request + " exception=" + e);
                ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), XADeliveryStage.this + "/visited, request=" + request + " exception=" + e + ", disconnecting");
                ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
            }
        });
        if (!listener)
            getStageQueue().enqueue(new StartStageRequest());
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/init done");
    }


    public void process(Request request) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/process, request=" + request);
        request.accept(visitor);
    }

    public void close() {
        if (closed.getAndSet(true))
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/close ...");
        super.close();
        closed.set(true);
        if (!notificationList.isEmpty()) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/close, final notify ...");
            notificationList.values().forEach(dr -> {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/close, final notify: " + dr);
                dr.callback.delivered(dr);
            });
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/close, final notify done");
            notificationList.clear();
        }
        if (!outboundTransactions.isEmpty()) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/close, passing prepared outbound tx to XAResourceManager ...");
            for (Iterator<Map.Entry<XidImpl, QueuePullTransaction>> iter = outboundTransactions.entrySet().iterator(); iter.hasNext(); ) {
                try {
                    Map.Entry<XidImpl, QueuePullTransaction> entry = iter.next();
                    XidImpl xid = entry.getKey();
                    XAContext xac = ctx.xaResourceManagerSwiftlet.createXAContext(xid);
                    int id = xac.register(toString());
                    QueuePullTransaction t = entry.getValue();
                    xac.addTransaction(id, t.getQueueName(), t);
                    xac.unregister(id, false);
                    xac.setPrepared(true);
                } catch (XAContextException e) {
                    ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), this + "/close, passing prepared outbound tx to XAResourceManager, exception: " + e);
                }
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/close, passing prepared outbound tx to XAResourceManager done");
            outboundTransactions.clear();
        }
        if (!inboundTransactions.isEmpty()) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/close, passing prepared inbound tx to XAResourceManager ...");
            inboundTransactions.forEach((key, value) -> value.handOver());
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/close, passing prepared inbound tx to XAResourceManager done");
        }
        producers.entrySet().forEach(entry -> {
            try {
                entry.getValue().close();
            } catch (Exception e) {
            }
        });
        producers.clear();
        consumers.entrySet().stream().forEach(entry -> {
            try {
                entry.getValue().close();
            } catch (Exception e) {
            }
        });
        consumers.clear();
        if (throttleQueue != null)
            throttleQueue.close();
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.SMQRFactory.START_STAGE_REQ, null);
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.SMQRFactory.SEND_ROUTE_REQ, null);
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.SMQRFactory.DELIVERY_REQ, null);
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.ROUTE_REQ, null);
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.ADJUST_REQ, null);
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.TRANSACTION_REQ, null);
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.COMMIT_REQ, null);
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.COMMIT_REPREQ, null);
    }

    public String toString(String direction) {
        return routingConnection.toString() + "/v942XADeliveryStage " + direction + ", recoveryBranchQ=" + recoveryBranchQ;
    }

    public String toString() {
        return routingConnection.toString() + "/v942XADeliveryStage, recoveryBranchQ=" + recoveryBranchQ;
    }

    private class Tx {
        XidImpl xid = null;
        Map<String, QueuePushTransaction> transactions = null;

        public Tx(XidImpl xid) {
            this.xid = xid;
            transactions = new HashMap();
        }

        QueuePushTransaction getTransaction(String queueName) {
            return transactions.get(queueName);
        }

        void addTransaction(String queueName, QueuePushTransaction t) {
            transactions.put(queueName, t);
        }

        void prepare() throws Exception {
            for (Iterator<Map.Entry<String, QueuePushTransaction>> iter = transactions.entrySet().iterator(); iter.hasNext(); ) {
                QueuePushTransaction t = (QueuePushTransaction) ((Map.Entry<?, ?>) iter.next()).getValue();
                try {
                    t.prepare(xid);
                } catch (Exception e) {
                    iter.remove();
                    if (!ctx.queueManager.isTemporaryQueue(t.getQueueName())) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/prepare, queue=" + t.getQueueName() + ", exception: " + e);
                        ctx.logSwiftlet.logWarning(ctx.routingSwiftlet.getName(), this + "/prepare, queue=" + t.getQueueName() + ", exception: " + e);
                        throw e;
                    }
                }
            }
        }

        void rollback() {
            transactions.entrySet().forEach(entry -> {
                try {
                    entry.getValue().rollback(xid, false);
                } catch (Exception e) {
                }
            });
            transactions.clear();
        }

        long commit() throws Exception {
            long fc = 0;
            for (Iterator<Map.Entry<String, QueuePushTransaction>> iter = transactions.entrySet().iterator(); iter.hasNext(); ) {
                QueuePushTransaction t = (QueuePushTransaction) ((Map.Entry<?, ?>) iter.next()).getValue();
                String name = t.getQueueName();
                try {
                    t.commit(xid);
                    fc = Math.max(fc, t.getFlowControlDelay());
                } catch (Exception e) {
                    iter.remove();
                    if (!ctx.queueManager.isTemporaryQueue(name)) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/commit, queue=" + name + ", exception: " + e);
                        ctx.logSwiftlet.logWarning(ctx.routingSwiftlet.getName(), this + "/commit, queue=" + name + ", exception: " + e);
                    }
                }
                if (ctx.queueManager.isTemporaryQueue(name)) {
                    QueueSender sender = producers.remove(name);
                    try {
                        sender.close();
                    } catch (Exception ignored) {
                    }
                }
            }
            transactions.clear();
            return fc;
        }

        void handOver() {
            try {
                XAContext xac = ctx.xaResourceManagerSwiftlet.createXAContext(xid);
                int id = xac.register(toString());
                for (Map.Entry<String, QueuePushTransaction> entry : transactions.entrySet()) {
                    QueuePushTransaction t = entry.getValue();
                    xac.addTransaction(id, t.getQueueName(), t);
                }
                transactions.clear();
                xac.unregister(id, false);
                xac.setPrepared(true);
            } catch (XAContextException e) {
                ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), this + "/handover, xid=" + xid + ", exception: " + e);
            }
        }

        public String toString() {
            return XADeliveryStage.this + "/Tx=" + xid;
        }
    }
}

