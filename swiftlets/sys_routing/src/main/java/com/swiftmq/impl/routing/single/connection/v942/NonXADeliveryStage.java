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
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.tools.requestreply.Request;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;
import java.util.*;

public class NonXADeliveryStage extends Stage {
    SMQRVisitor visitor = null;
    boolean listener = false;
    int txNo = 0;
    Map outboundTransactions = null;
    Map producers = null;
    Map consumers = null;
    Map notificationList = null;
    boolean closed = false;
    ThrottleQueue throttleQueue = null;

    public NonXADeliveryStage(SwiftletContext ctx, RoutingConnection routingConnection) {
        super(ctx, routingConnection);
        visitor = routingConnection.getVisitor();
        listener = routingConnection.isListener();
        outboundTransactions = new HashMap();
        notificationList = new HashMap();
        producers = new HashMap();
        consumers = new HashMap();
        if (ctx.inboundFCEnabled)
            throttleQueue = new ThrottleQueue(ctx, routingConnection);

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/created");
        try {
            producers.put(RoutingSwiftletImpl.UNROUTABLE_QUEUE, ctx.queueManager.createQueueSender(RoutingSwiftletImpl.UNROUTABLE_QUEUE, null));
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/Exception creating unroutable sender: " + e);
        }
    }

    private long processTransactionRequest(NonXATransactionRequest request) throws Exception {
        long delay = 0;
        int txNo = request.getSequenceNo();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString("INBOUND") + "/processTransactionRequest, txNo=" + txNo + " ...");
        Tx tx = new Tx(txNo);
        List messageList = request.getMessageList();
        for (int i = 0; i < messageList.size(); i++) {
            boolean msgValid = true;
            MessageImpl msg = (MessageImpl) messageList.get(i);
            String queueName = null;
            if (msg.getDestRouter().equals(ctx.routerName))
                queueName = msg.getDestQueue();
            else
                queueName = SchedulerRegistry.QUEUE_PREFIX + msg.getDestRouter();
            QueueSender sender = (QueueSender) producers.get(queueName);
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
                        sender = (QueueSender) producers.get(RoutingSwiftletImpl.UNROUTABLE_QUEUE);
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
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/init...");
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.SMQRFactory.START_STAGE_REQ, new RequestHandler() {
            public void visited(Request request) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + "...");
                int txSize = ((Integer) routingConnection.getEntity().getProperty("inbound-transaction-size").getValue()).intValue();
                int windowSize = ((Integer) routingConnection.getEntity().getProperty("inbound-window-size").getValue()).intValue();
                AdjustRequest rc = new AdjustRequest(txSize, windowSize);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + ", sending request=" + rc);
                routingConnection.getOutboundQueue().enqueue(rc);
                routingConnection.setXaSelected(false);
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.SMQRFactory.SEND_ROUTE_REQ, new RequestHandler() {
            public void visited(Request request) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + "...");
                RouteRequest rc = new RouteRequest(((SendRouteRequest) request).getRoute());
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + ", sending request=" + rc);
                routingConnection.getOutboundQueue().enqueue(rc);
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.ROUTE_REQ, new RequestHandler() {
            public void visited(Request request) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("INBOUND") + "/visited, request=" + request + "...");
                RouteRequest rc = (RouteRequest) request;
                try {
                    ctx.routeExchanger.processRoute(routingConnection, rc.getRoute(ctx.routeExchanger.getRouteConverter()));
                } catch (Exception e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("INBOUND") + "/visited, request=" + request + " exception=" + e);
                    ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString() + "/visited, request=" + request + " exception=" + e);
                }
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.ADJUST_REQ, new RequestHandler() {
            public void visited(Request request) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString() + "/visited, request=" + request + "...");
                AdjustRequest rc = (AdjustRequest) request;
                routingConnection.setTransactionSize(rc.getTransactionSize());
                routingConnection.setWindowSize(rc.getWindowSize());
                // A listener must wait until the connector sends a request.
                // It then sends a request by itself to ensure the NonXADeliveryStage is active at the connector side.
                if (listener)
                    getStageQueue().enqueue(new StartStageRequest());
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString() + "/visited, request=" + request + ", activating connection");
                routingConnection.getActivationListener().activated(routingConnection);
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.SMQRFactory.DELIVERY_REQ, new RequestHandler() {
            public void visited(Request request) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + "...");
                try {
                    DeliveryRequest rc = (DeliveryRequest) request;
                    QueuePullTransaction srcTx = rc.readTransaction;
                    QueueReceiver receiver = (QueueReceiver) consumers.get(srcTx.getQueueName());
                    if (receiver == null) {
                        receiver = ctx.queueManager.createQueueReceiver(srcTx.getQueueName(), null, null);
                        consumers.put(srcTx.getQueueName(), receiver);
                    }
                    QueuePullTransaction destTx = receiver.createTransaction(true);  // mark redelivered on rollback
                    List al = new ArrayList(rc.len);
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
                    txNo++;
                    if (txNo == Integer.MAX_VALUE)
                        txNo = 0;
                    outboundTransactions.put(txNo, destTx);
                    NonXATransactionRequest txr = new NonXATransactionRequest(txNo, al);
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + " sending request=" + txr);
                    routingConnection.getOutboundQueue().enqueue(txr);
                    if (outboundTransactions.size() <= routingConnection.getWindowSize())
                        rc.callback.delivered(rc);
                    else
                        notificationList.put(txNo, rc);
                } catch (Exception e) {
                    e.printStackTrace();
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + " exception=" + e);
                    ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString() + "/visited, request=" + request + " exception=" + e + ", disconnecting");
                    ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
                }
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.NONXA_COMMIT_REQ, new RequestHandler() {
            public void visited(Request request) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + "...");
                NonXACommitRequest rc = (NonXACommitRequest) request;
                int sequenceNo = rc.getSequenceNo();
                QueuePullTransaction t = (QueuePullTransaction) outboundTransactions.remove(sequenceNo);
                try {
                    t.commit();
                    DeliveryRequest dr = (DeliveryRequest) notificationList.remove(sequenceNo);
                    if (dr != null) {
                        dr.callback.delivered(dr);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString("OUTBOUND") + "/visited, request=" + request + " exception=" + e);
                    ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString() + "/visited, request=" + request + " exception=" + e + ", disconnecting");
                    ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
                }
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.NONXA_TRANSACTION_REQ, new RequestHandler() {
            public void visited(Request request) {
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
                    ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), NonXADeliveryStage.this.toString() + "/visited, request=" + request + " exception=" + e + ", disconnecting");
                    ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
                }
            }
        });
        if (!listener)
            getStageQueue().enqueue(new StartStageRequest());
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/init done");
    }

    public void process(Request request) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/process, request=" + request);
        request.accept(visitor);
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/close ...");
        super.close();
        closed = true;
        if (notificationList.size() > 0) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/close, final notify ...");
            for (Iterator iter = notificationList.entrySet().iterator(); iter.hasNext(); ) {
                Map.Entry entry = (Map.Entry) iter.next();
                DeliveryRequest dr = (DeliveryRequest) entry.getValue();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/close, final notify: " + dr);
                dr.callback.delivered(dr);
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/close, final notify done");
            notificationList.clear();
        }
        if (outboundTransactions.size() > 0) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/close, rollback open outbound tx ...");
            for (Iterator iter = outboundTransactions.entrySet().iterator(); iter.hasNext(); ) {
                try {
                    Map.Entry entry = (Map.Entry) iter.next();
                    QueuePullTransaction t = (QueuePullTransaction) entry.getValue();
                    t.rollback();
                } catch (Exception e) {
                    ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), toString() + "/close, rollback open outbound tx, exception: " + e);
                }
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/close, rollback open outbound tx done");
            outboundTransactions.clear();
        }
        for (Iterator iter = producers.entrySet().iterator(); iter.hasNext(); ) {
            QueueSender sender = (QueueSender) ((Map.Entry) iter.next()).getValue();
            try {
                sender.close();
            } catch (Exception e) {
            }
        }
        producers.clear();
        for (Iterator iter = consumers.entrySet().iterator(); iter.hasNext(); ) {
            QueueReceiver receiver = (QueueReceiver) ((Map.Entry) iter.next()).getValue();
            try {
                receiver.close();
            } catch (Exception e) {
            }
        }
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
        Map transactions = null;

        public Tx(int txNo) {
            this.txNo = txNo;
            transactions = new HashMap();
        }

        QueuePushTransaction getTransaction(String queueName) {
            return (QueuePushTransaction) transactions.get(queueName);
        }

        void addTransaction(String queueName, QueuePushTransaction t) {
            transactions.put(queueName, t);
        }

        void rollback() {
            for (Iterator iter = transactions.entrySet().iterator(); iter.hasNext(); ) {
                QueuePushTransaction t = (QueuePushTransaction) ((Map.Entry) iter.next()).getValue();
                try {
                    t.rollback();
                } catch (Exception e) {
                }
            }
            transactions.clear();
        }

        long commit() throws Exception {
            long fc = 0;
            for (Iterator iter = transactions.entrySet().iterator(); iter.hasNext(); ) {
                QueuePushTransaction t = (QueuePushTransaction) ((Map.Entry) iter.next()).getValue();
                String name = t.getQueueName();
                try {
                    t.commit();
                    fc = Math.max(fc, t.getFlowControlDelay());
                } catch (Exception e) {
                    iter.remove();
                    if (!ctx.queueManager.isTemporaryQueue(name)) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/commit, queue=" + name + ", exception: " + e);
                        ctx.logSwiftlet.logWarning(ctx.routingSwiftlet.getName(), toString() + "/commit, queue=" + name + ", exception: " + e);
                    }
                }
                if (ctx.queueManager.isTemporaryQueue(name)) {
                    QueueSender sender = (QueueSender) producers.remove(name);
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
            return NonXADeliveryStage.this.toString() + "/Tx=" + txNo;
        }
    }
}

