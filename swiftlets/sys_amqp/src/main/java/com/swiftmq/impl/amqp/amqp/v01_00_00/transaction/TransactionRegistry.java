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

package com.swiftmq.impl.amqp.amqp.v01_00_00.transaction;

import com.swiftmq.amqp.v100.generated.messaging.delivery_state.DeliveryStateIF;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.Released;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TransactionError;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TransactionId;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TxnIdIF;
import com.swiftmq.amqp.v100.generated.transport.definitions.AmqpError;
import com.swiftmq.amqp.v100.generated.transport.performatives.DispositionFrame;
import com.swiftmq.amqp.v100.types.AMQPString;
import com.swiftmq.impl.amqp.SwiftletContext;
import com.swiftmq.impl.amqp.amqp.v01_00_00.*;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.swiftlet.queue.MessageIndex;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionRegistry {
    private static Released RELEASED = new Released();
    SwiftletContext ctx;
    SessionHandler sessionHandler;
    long startupTime;
    long lastId;
    Lock lock = new ReentrantLock();
    Map activeTx = new HashMap();

    public TransactionRegistry(SwiftletContext ctx, SessionHandler sessionHandler) {
        this.ctx = ctx;
        this.sessionHandler = sessionHandler;
        startupTime = System.currentTimeMillis();
        lastId = 0;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/created");
    }

    public TxnIdIF createTxId() {
        lock.lock();
        try {
            StringBuffer b = new StringBuffer();
            b.append(startupTime);
            b.append('-');
            b.append(++lastId);
            String id = b.toString();
            activeTx.put(id, new ActiveTxEntry());
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/createTxId: " + id);
            return new TransactionId(id.getBytes());
        } finally {
            lock.unlock();
        }
    }

    public void addToTransaction(TxnIdIF btxnId, String linkName, MessageImpl message, TargetLink targetLink) throws EndWithErrorException {
        String txnId = new String(((TransactionId) btxnId).getValue());
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/addToTransaction, txnId=" + txnId + ", linkName=" + linkName + ", message=" + message);
        lock.lock();
        try {
            ActiveTxEntry activeTxEntry = (ActiveTxEntry) activeTx.get(txnId);
            if (activeTxEntry == null) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/addToTransaction, txnId=" + txnId + ", not found!");
                throw new SessionEndException(TransactionError.UNKNOWN_ID, new AMQPString("Transaction id not found: " + txnId));
            }
            if (activeTxEntry.inboundTxEntryMap == null)
                activeTxEntry.inboundTxEntryMap = new HashMap();
            InboundTxEntry inboundTxEntry = (InboundTxEntry) activeTxEntry.inboundTxEntryMap.get(linkName);
            try {
                if (inboundTxEntry == null) {
                    QueuePushTransaction tx = targetLink.getQueueSender().createTransaction();
                    targetLink.increaseActiveTransactions();
                    inboundTxEntry = new InboundTxEntry(tx, targetLink);
                    activeTxEntry.inboundTxEntryMap.put(linkName, inboundTxEntry);
                }
                ((QueuePushTransaction) inboundTxEntry.tx).putMessage(message);
            } catch (QueueException e) {
                throw new ConnectionEndException(AmqpError.INTERNAL_ERROR, new AMQPString(e.toString()));
            }
        } finally {
            lock.unlock();
        }
    }

    public void addToTransaction(TxnIdIF btxnId, SourceLink sourceLink, long deliveryId, MessageIndex messageIndex, long size) throws EndWithErrorException {
        String txnId = new String(((TransactionId) btxnId).getValue());
        String linkName = sourceLink.getName();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/addToTransaction, txnId=" + txnId + ", linkName=" + linkName + ", messageIndex=" + messageIndex);
        lock.lock();
        try {
            ActiveTxEntry activeTxEntry = (ActiveTxEntry) activeTx.get(txnId);
            if (activeTxEntry == null) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/addToTransaction, txnId=" + txnId + ", not found!");
                throw new SessionEndException(TransactionError.UNKNOWN_ID, new AMQPString("Transaction id not found: " + txnId));
            }
            sourceLink.increaseActiveTransactions();
            if (activeTxEntry.outboundTxEntryMap == null)
                activeTxEntry.outboundTxEntryMap = new HashMap();
            OutboundTxEntry outboundTxEntry = (OutboundTxEntry) activeTxEntry.outboundTxEntryMap.get(deliveryId);
            if (outboundTxEntry == null) {
                outboundTxEntry = new OutboundTxEntry(sourceLink, messageIndex, null);
                activeTxEntry.outboundTxEntryMap.put(deliveryId, outboundTxEntry);
            } else {
                outboundTxEntry.sourceLink = sourceLink;
                outboundTxEntry.messageIndex = messageIndex;
            }
        } finally {
            lock.unlock();
        }
    }

    public void addToTransaction(TxnIdIF txnIdIF, DispositionFrame frame) throws EndWithErrorException {
        String txnId = new String(((TransactionId) txnIdIF).getValue());
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/addToTransaction, txnId=" + txnId + ", frame=" + frame);
        long from = frame.getFirst().getValue();
        long to = from;
        if (frame.getLast() != null)
            to = frame.getLast().getValue();
        DeliveryStateIF deliveryStateIF = frame.getState();
        long current = from;
        while (current <= to) {
            addToTransaction(txnId, current, deliveryStateIF);
            current++;
        }
    }

    private void addToTransaction(String txnId, long deliveryId, DeliveryStateIF deliveryStateIF) throws EndWithErrorException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/addToTransaction, txnId=" + txnId + ", deliveryId=" + deliveryId + ", deliveryStateIF=" + deliveryStateIF);
        lock.lock();
        try {
            ActiveTxEntry activeTxEntry = (ActiveTxEntry) activeTx.get(txnId);
            if (activeTxEntry == null) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/addToTransaction, txnId=" + txnId + ", not found!");
                throw new SessionEndException(TransactionError.UNKNOWN_ID, new AMQPString("Transaction id not found: " + txnId));
            }
            if (activeTxEntry.outboundTxEntryMap == null)
                activeTxEntry.outboundTxEntryMap = new HashMap();
            OutboundTxEntry outboundTxEntry = (OutboundTxEntry) activeTxEntry.outboundTxEntryMap.get(deliveryId);
            if (outboundTxEntry == null) {
                outboundTxEntry = new OutboundTxEntry(null, null, deliveryStateIF);
                activeTxEntry.outboundTxEntryMap.put(deliveryId, outboundTxEntry);
            } else
                outboundTxEntry.deliveryStateIF = deliveryStateIF;
        } finally {
            lock.unlock();
        }
    }

    public void discharge(TxnIdIF btxnId, boolean rollback) throws EndWithErrorException {
        String txnId = new String(((TransactionId) btxnId).getValue());
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/discharge, txnId=" + txnId + ", rollback=" + rollback);
        lock.lock();
        try {
            ActiveTxEntry activeTxEntry = (ActiveTxEntry) activeTx.remove(txnId);
            if (activeTxEntry == null) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/discharge, txnId=" + txnId + ", not found!");
                throw new SessionEndException(TransactionError.UNKNOWN_ID, new AMQPString("Transaction id not found: " + txnId));
            }
            if (activeTxEntry.inboundTxEntryMap != null) {
                for (Iterator iter = activeTxEntry.inboundTxEntryMap.entrySet().iterator(); iter.hasNext(); ) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    InboundTxEntry inboundTxEntry = (InboundTxEntry) entry.getValue();
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/discharge txnid=" + txnId + ", linkName=" + entry.getKey());
                    if (rollback)
                        inboundTxEntry.tx.rollback();
                    else
                        inboundTxEntry.tx.commit();
                    inboundTxEntry.targetLink.setFlowcontrolDelay(inboundTxEntry.tx.getFlowControlDelay());
                    inboundTxEntry.targetLink.decreaseActiveTransactions();
                    inboundTxEntry.targetLink.closeResource();
                }
                activeTxEntry.inboundTxEntryMap.clear();
            }
            if (activeTxEntry.outboundTxEntryMap != null) {
                for (Iterator iter = activeTxEntry.outboundTxEntryMap.entrySet().iterator(); iter.hasNext(); ) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    long deliveryId = ((Long) entry.getKey()).longValue();
                    OutboundTxEntry outboundTxEntry = (OutboundTxEntry) entry.getValue();
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/discharge txnid=" + txnId + ", deliveryId=" + deliveryId);
                    if (rollback) {
                        if (outboundTxEntry.sourceLink != null)
                            sessionHandler.settleOutbound(deliveryId, RELEASED);
                    } else
                        sessionHandler.settleOutbound(deliveryId, outboundTxEntry.deliveryStateIF);
                    if (outboundTxEntry.sourceLink != null) {
                        outboundTxEntry.sourceLink.decreaseActiveTransactions();
                        outboundTxEntry.sourceLink.closeResource();
                    }
                }
                activeTxEntry.outboundTxEntryMap.clear();
                sessionHandler.sendOutboundDeliveries();
            }
        } catch (QueueException e) {
            throw new ConnectionEndException(AmqpError.INTERNAL_ERROR, new AMQPString(e.toString()));
        } finally {
            lock.unlock();
        }
    }

    public void close() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/close");
        lock.lock();
        try {
            Map cloned = (Map) ((HashMap) activeTx).clone();
            for (Iterator iter = cloned.entrySet().iterator(); iter.hasNext(); ) {
                Map.Entry entry = (Map.Entry) iter.next();
                try {
                    discharge(new TransactionId(((String) entry.getKey()).getBytes()), true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            activeTx.clear();
        } finally {
            lock.unlock();
        }
    }

    public String toString() {
        return sessionHandler.toString() + "/TransactionRegistry";
    }

    private class ActiveTxEntry {
        Map inboundTxEntryMap = null;
        Map outboundTxEntryMap = null;
    }

    private class InboundTxEntry {
        QueuePushTransaction tx;
        TargetLink targetLink;

        private InboundTxEntry(QueuePushTransaction tx, TargetLink targetLink) {
            this.tx = tx;
            this.targetLink = targetLink;
        }
    }

    private class OutboundTxEntry {
        SourceLink sourceLink;
        MessageIndex messageIndex;
        DeliveryStateIF deliveryStateIF;

        private OutboundTxEntry(SourceLink sourceLink, MessageIndex messageIndex, DeliveryStateIF deliveryStateIF) {
            this.sourceLink = sourceLink;
            this.messageIndex = messageIndex;
            this.deliveryStateIF = deliveryStateIF;
        }
    }
}
