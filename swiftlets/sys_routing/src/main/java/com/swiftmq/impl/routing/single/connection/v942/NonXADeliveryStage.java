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
import com.swiftmq.impl.routing.single.smqpr.DeliveryRequest;
import com.swiftmq.impl.routing.single.smqpr.SMQRVisitor;
import com.swiftmq.impl.routing.single.smqpr.SendRouteRequest;
import com.swiftmq.impl.routing.single.smqpr.StartStageRequest;
import com.swiftmq.impl.routing.single.smqpr.v942.*;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.tools.concurrent.AtomicWrappingCounterInteger;
import com.swiftmq.tools.requestreply.Request;

import java.util.*;

public class NonXADeliveryStage extends Stage {
    SMQRVisitor visitor = null;
    boolean listener = false;
    AtomicWrappingCounterInteger txNo = new AtomicWrappingCounterInteger(0);
    Map<Integer, QueuePullTransaction> outboundTransactions = null;
    Map<String, QueueSender> producers = null;
    Map<String, QueueReceiver> consumers = null;
    Map<Integer, DeliveryRequest> notificationList = null;
    boolean closed = false;
    ThrottleQueue throttleQueue = null;

    public NonXADeliveryStage(SwiftletContext ctx, RoutingConnection routingConnection) {
        super(ctx, routingConnection);
        visitor = routingConnection.getVisitor();
        listener = routingConnection.isListener();
        outboundTransactions = new HashMap<>();
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
    }

    private long processTransactionRequest(NonXATransactionRequest request) throws Exception {
        long delay = 0;
        int txNo = request.getSequenceNo();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString("INBOUND") + "/processTransactionRequest, txNo=" + txNo + " ...");
        Tx tx = new Tx(txNo);
        List messageList = request.getMessageList();
        for (Object o : messageList) {
            boolean msgValid = true;
            MessageImpl msg = (MessageImpl) o;
            String queueName = null;
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
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString("INBOUND") + "/processTransactionRequest, txNo=" + txNo + ", exception creating sender, queue=" + queueName);
                    if (ctx.queueManager.isTemporaryQueue(queueName)) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString("INBOUND") + "/processTransactionRequest, txNo=" + txNo + ", temp queue, forget it");
                        msgValid = false;
                    } else {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString("INBOUND") + "/processTransactionRequest, txNo=" + txNo + ", using unroutable queue");
                        sender = (QueueSender) producers.get(RoutingSwiftletImpl.UNROUTABLE_QUEUE);
                        msg.setStringProperty(MessageImpl.PROP_UNROUTABLE_REASON, e.toString());
                    }
                }
            }
            if (msgValid) {
                try {
                    // Check redelivered and activate duplicate message detection for this message by setting the DOUBT property
                    if (msg.getJMSRedelivered()) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString("INBOUND") + "/processTransactionRequest, txNo=" + txNo + ", message is redelivered, activating dups detection for it");
                        msg.setJMSRedelivered(false);
                        msg.setBooleanProperty(MessageImpl.PROP_DOUBT_DUPLICATE, true);
                    }
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
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString("INBOUND") + "/processTransactionRequest, txNo=" + txNo + ", exception put message, queue=" + queueName);
                    if (ctx.queueManager.isTemporaryQueue(queueName)) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString("INBOUND") + "/processTransactionRequest, txNo=" + txNo + ", temp queue, forget it");
                    } else {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString("INBOUND") + "/processTransactionRequest, txNo=" + txNo + ", using unroutable queue");
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
            delay = tx.commit();
        } catch (Exception e) {
            tx.rollback();
            throw e;
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString("INBOUND") + "/processTransactionRequest, txNo=" + txNo + " done");
        return delay;
    }


    protected void init() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/init...");
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.SMQRFactory.START_STAGE_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + "...");
            int txSize = (Integer) routingConnection.getEntity().getProperty("inbound-transaction-size").getValue();
            int windowSize = (Integer) routingConnection.getEntity().getProperty("inbound-window-size").getValue();
            AdjustRequest rc = new AdjustRequest(txSize, windowSize);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + ", sending request=" + rc);
            routingConnection.getOutboundQueue().enqueue(rc);
            routingConnection.setXaSelected(false);
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.SMQRFactory.SEND_ROUTE_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + "...");
            RouteRequest rc = new RouteRequest(((SendRouteRequest) request).getRoute());
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + ", sending request=" + rc);
            routingConnection.getOutboundQueue().enqueue(rc);
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.ROUTE_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("INBOUND") + "/visited, request=" + request + "...");
            RouteRequest rc = (RouteRequest) request;
            try {
                ctx.routeExchanger.processRoute(routingConnection, rc.getRoute(ctx.routeExchanger.getRouteConverter()));
            } catch (Exception e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("INBOUND") + "/visited, request=" + request + " exception=" + e);
                ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this + "/visited, request=" + request + " exception=" + e);
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.ADJUST_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this + "/visited, request=" + request + "...");
            AdjustRequest rc = (AdjustRequest) request;
            routingConnection.setTransactionSize(rc.getTransactionSize());
            routingConnection.setWindowSize(rc.getWindowSize());
            // A listener must wait until the connector sends a request.
            // It then sends a request by itself to ensure the NonXADeliveryStage is active at the connector side.
            if (listener)
                getStageQueue().enqueue(new StartStageRequest());
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this + "/visited, request=" + request + ", activating connection");
            routingConnection.getActivationListener().activated(routingConnection);
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.SMQRFactory.DELIVERY_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + "...");
            try {
                DeliveryRequest rc = (DeliveryRequest) request;
                QueuePullTransaction srcTx = rc.readTransaction;
                QueueReceiver receiver = consumers.get(srcTx.getQueueName());
                if (receiver == null) {
                    receiver = ctx.queueManager.createQueueReceiver(srcTx.getQueueName(), null, null);
                    consumers.put(srcTx.getQueueName(), receiver);
                }
                QueuePullTransaction destTx = receiver.createTransaction(true);  // mark redelivered on rollback
                List<MessageImpl> al = new ArrayList<>();
                for (int i = 0; i < rc.len; i++) {
                    MessageIndex messageIndex = rc.entries[i].getMessageIndex();
                    destTx.moveToTransaction(messageIndex, srcTx);
                    MessageImpl msg = rc.entries[i].getMessage();
                    if (msg.getSourceRouter() == null)
                        msg.setSourceRouter(ctx.routerName);
                    if (msg.getDestRouter() == null)
                        msg.setDestRouter(rc.destinationRouter);
                    msg.setJMSRedelivered(messageIndex.getDeliveryCount() > 1); // mark redelivered for dups detection at the receiving router
                    al.add(msg);
                }
                txNo.getAndIncrement();
                outboundTransactions.put(txNo.get(), destTx);
                NonXATransactionRequest txr = new NonXATransactionRequest(txNo.get(), al);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + " sending request=" + txr);
                routingConnection.getOutboundQueue().enqueue(txr);
                if (outboundTransactions.size() <= routingConnection.getWindowSize())
                    rc.callback.delivered(rc);
                else
                    notificationList.put(txNo.get(), rc);
            } catch (Exception e) {
                e.printStackTrace();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + " exception=" + e);
                ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this + "/visited, request=" + request + " exception=" + e + ", disconnecting");
                ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.NONXA_COMMIT_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + "...");
            NonXACommitRequest rc = (NonXACommitRequest) request;
            int sequenceNo = rc.getSequenceNo();
            QueuePullTransaction t = outboundTransactions.remove(sequenceNo);
            try {
                t.commit();
                DeliveryRequest dr = notificationList.remove(sequenceNo);
                if (dr != null) {
                    dr.callback.delivered(dr);
                }
            } catch (Exception e) {
                e.printStackTrace();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + " exception=" + e);
                ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this + "/visited, request=" + request + " exception=" + e + ", disconnecting");
                ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.NONXA_TRANSACTION_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("INBOUND") + "/visited, request=" + request + "...");
            NonXATransactionRequest rc = (NonXATransactionRequest) request;
            try {
                long delay = processTransactionRequest(rc);
                NonXACommitRequest cr = new NonXACommitRequest(rc.getSequenceNo());
                if (throttleQueue != null) {
                    if (delay > 0) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("INBOUND") + "/visited, request=" + request + " enqueue throttle request with delay=" + delay);
                        throttleQueue.enqueue(new ThrottleRequest(delay));
                    }
                    throttleQueue.enqueue(cr);
                } else
                    routingConnection.getOutboundQueue().enqueue(cr);
            } catch (Exception e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("INBOUND") + "/visited, request=" + request + " exception=" + e);
                ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this + "/visited, request=" + request + " exception=" + e + ", disconnecting");
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
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/close ...");
        super.close();
        closed = true;
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
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/close, rollback open outbound tx ...");
            for (Iterator<Map.Entry<Integer, QueuePullTransaction>> iter = outboundTransactions.entrySet().iterator(); iter.hasNext(); ) {
                try {
                    Map.Entry<Integer, QueuePullTransaction> entry = iter.next();
                    QueuePullTransaction t = entry.getValue();
                    t.rollback();
                } catch (Exception e) {
                    ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), this + "/close, rollback open outbound tx, exception: " + e);
                }
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/close, rollback open outbound tx done");
            outboundTransactions.clear();
        }
        producers.values().forEach(sender -> {
            try {
                sender.close();
            } catch (Exception e) {
            }
        });
        producers.clear();
        consumers.values().forEach(receiver -> {
            try {
                receiver.close();
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
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.NONXA_TRANSACTION_REQ, null);
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.NONXA_COMMIT_REQ, null);
    }

    public String toString(String direction) {
        return routingConnection.toString() + "/v942NonXADeliveryStage " + direction;
    }

    public String toString() {
        return routingConnection.toString() + "/v942NonXADeliveryStage";
    }

    private class Tx {
        int txNo;
        Map<String, QueuePushTransaction> transactions = null;

        public Tx(int txNo) {
            this.txNo = txNo;
            transactions = new HashMap<>();
        }

        QueuePushTransaction getTransaction(String queueName) {
            return transactions.get(queueName);
        }

        void addTransaction(String queueName, QueuePushTransaction t) {
            transactions.put(queueName, t);
        }

        void rollback() {
            transactions.forEach((key, value) -> {
                try {
                    value.rollback();
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
                    t.commit();
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

        public String toString() {
            return NonXADeliveryStage.this + "/Tx=" + txNo;
        }
    }
}

