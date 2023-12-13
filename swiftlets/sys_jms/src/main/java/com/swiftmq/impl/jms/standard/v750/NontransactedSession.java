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

package com.swiftmq.impl.jms.standard.v750;

import com.swiftmq.jms.smqp.v750.*;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.MessageIndex;
import com.swiftmq.swiftlet.queue.MessageProcessor;
import com.swiftmq.swiftlet.queue.QueuePullTransaction;
import com.swiftmq.swiftlet.queue.QueueTransactionClosedException;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;
import com.swiftmq.tools.concurrent.CallbackJoin;
import com.swiftmq.tools.queue.SingleProcessorQueue;
import com.swiftmq.tools.requestreply.GenericRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NontransactedSession extends Session {
    protected List<MessageDeliveredRequest> deliveredList = null;

    public NontransactedSession(String connectionTracePrefix, Entity sessionEntity, SingleProcessorQueue connectionOutboundQueue, int dispatchId, ActiveLogin activeLogin, int ackMode) {
        super(connectionTracePrefix, sessionEntity, connectionOutboundQueue, dispatchId, activeLogin, ackMode);
        ctx.transacted = false;
        deliveredList = new ArrayList<>();
    }

    public void visit(MessageDeliveredRequest req) {
        if (closed || recoveryInProgress)
            return;
        if (!req.isDuplicate())
            deliveredList.add(req);
        super.visit(req);
    }

    public void visit(AcknowledgeMessageRequest req) {
        if (closed)
            return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitAcknowledgeMessageRequest");
        AcknowledgeMessageReply reply = null;
        if (req.isReplyRequired()) {
            reply = (AcknowledgeMessageReply) req.createReply();
            reply.setOk(true);
        }

        if (ctx.ackMode == javax.jms.Session.CLIENT_ACKNOWLEDGE) {
            Map<Consumer, ConsumerAckEntry> ackConsumers = new HashMap<>();
            for (MessageDeliveredRequest request : deliveredList) {
                Consumer consumer = consumerList.get(request.getQueueConsumerId());
                MessageIndex actIndex = request.getMessageIndex();
                QueuePullTransaction t = consumer.getTransaction();
                ConsumerAckEntry ackEntry = (ConsumerAckEntry) ackConsumers.get(consumer);
                if (ackEntry == null) {
                    ackEntry = new ConsumerAckEntry(t);
                    ackConsumers.put(consumer, ackEntry);
                }
                ackEntry.ackList.add(actIndex);
            }
            deliveredList.clear();
            boolean callbackRegistered = false;
            MultiAckJoin join = new MultiAckJoin(reply);
            for (Map.Entry<Consumer, ConsumerAckEntry> ackEntryEntry : ackConsumers.entrySet()) {
                Consumer consumer = (Consumer) ((Map.Entry<?, ?>) ackEntryEntry).getKey();
                ConsumerAckEntry ackEntry = (ConsumerAckEntry) ((Map.Entry<?, ?>) ackEntryEntry).getValue();
                try {
                    join.incNumberCallbacks();
                    ackEntry.transaction.acknowledgeMessages(ackEntry.ackList, new MultiAckCallback(join, consumer, ackEntry.ackList.size()));
                    callbackRegistered = true;
                } catch (QueueTransactionClosedException e) {
                }
            }
            join.setBlocked(false);
            if (!callbackRegistered && reply != null)
                reply.send();
        } else {
            MessageIndex ackIndex = req.getMessageIndex();
            Consumer consumer = (Consumer) consumerList.get(req.getQueueConsumerId());
            ackSingleMessage(reply, ackIndex, consumer);
        }
    }

    private static class ConsumerAckEntry {
        QueuePullTransaction transaction = null;
        List<MessageIndex> ackList = new ArrayList<>();

        private ConsumerAckEntry(QueuePullTransaction transaction) {
            this.transaction = transaction;
        }
    }

    private void ackSingleMessage(AcknowledgeMessageReply reply, MessageIndex ackIndex, Consumer consumer) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/ackSingleMessage");
        QueuePullTransaction t = consumer.getReadTransaction();
        try {
            if (!t.isClosed())
                t.acknowledgeMessage(ackIndex, new SingleAckCallback(reply, consumer));
            else {
                // Always send a reply, otherwise ConnectionConsumers will be blocked.
                if (reply != null)
                    reply.send();
            }
        } catch (Exception e) {
            // temp queue might be deleted in the mean time
            if (reply != null)
                reply.send();
        }
    }

    private class SingleAckCallback extends AsyncCompletionCallback {
        AcknowledgeMessageReply reply = null;
        Consumer consumer = null;

        private SingleAckCallback(AcknowledgeMessageReply reply, Consumer consumer) {
            this.reply = reply;
            this.consumer = consumer;
        }

        public void done(boolean success) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/SingleAckCallback, success=" + success);
            if (success) {
                Long size = (Long) getResult();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/SingleAckCallback, success=" + success + ", size=" + size);
            } else {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/SingleAckCallback, success=" + success + ", exception=" + getException());
                if (reply != null) {
                    reply.setOk(false);
                    reply.setException(getException());
                }
            }
            if (reply != null)
                reply.send();
        }
    }

    private class MultiAckJoin extends CallbackJoin {
        AcknowledgeMessageReply reply = null;

        private MultiAckJoin(AcknowledgeMessageReply reply) {
            this.reply = reply;
        }

        protected void callbackDone(AsyncCompletionCallback callback, boolean success, boolean last) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/MultiAckJoin, callback=" + callback + ", success=" + success + ", last=" + last);
            if (last && reply != null) {
                reply.setOk(true);
                reply.send();
            }
        }
    }

    private class MultiAckCallback extends AsyncCompletionCallback {
        MultiAckJoin join = null;
        Consumer consumer = null;
        int nMsgs = 0;

        private MultiAckCallback(MultiAckJoin join, Consumer consumer, int nMsgs) {
            this.join = join;
            this.consumer = consumer;
            this.nMsgs = nMsgs;
        }

        public void done(boolean success) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/MultiAckCallback, success=" + success);
            if (success) {
                Long size = (Long) getResult();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/MultiAckCallback, success=" + success + ", nMsgs=" + nMsgs + ", size=" + size);
            }
            join.done(this, success);
        }
    }

    public void visitGenericRequest(GenericRequest request) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitGenericRequest/RecoverSessionReply");
        RecoverSessionReply reply = (RecoverSessionReply) request.getPayload();
        recoveryEpoche = reply.getRecoveryEpoche();
        try {
            for (int i = 0; i < consumerList.size(); i++) {
                Consumer consumer = consumerList.get(i);
                if (consumer != null) {
                    consumer.createReadTransaction();
                    consumer.createTransaction();
                    AsyncMessageProcessor mp = (AsyncMessageProcessor) consumer.getMessageProcessor();
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitGenericRequest/RecoverSessionReply, get message processor: " + mp);
                    if (mp != null) {
                        int maxBulkSize = (int) (mp.getMaxBulkSize() / 1024);
                        mp = new AsyncMessageProcessor(this, ctx, consumer, mp.getConsumerCacheSize(), recoveryEpoche);
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitGenericRequest/RecoverSessionReply, new message processor: " + mp);
                        mp.setMaxBulkSize(maxBulkSize);
                        consumer.setMessageListener(consumer.getClientDispatchId(), consumer.getClientListenerId(), mp);
                        //           consumer.getReadTransaction().registerMessageProcessor(mp);
                    }
                }
            }
        } catch (Exception e) {
            reply.setOk(false);
            reply.setException(e);
        }
        recoveryInProgress = false;
        reply.send();
    }

    public void visit(RecoverSessionRequest req) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitRecoverSessionRequest");
        recoveryInProgress = true;
        RecoverSessionReply reply = (RecoverSessionReply) req.createReply();
        reply.setRecoveryEpoche(req.getRecoveryEpoche());
        reply.setOk(true);
        for (int i = 0; i < consumerList.size(); i++) {
            Consumer consumer = consumerList.get(i);
            if (consumer != null) {
                try {
                    MessageProcessor mp = consumer.getMessageProcessor();
                    if (mp != null) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitRecoverSessionRequest, stopping message processor: " + mp);
                        mp.stop();
                        consumer.getReadTransaction().unregisterMessageProcessor(mp);
                    }
                    consumer.getReadTransaction().rollback();
                    consumer.getTransaction().rollback();
                } catch (Exception e) {
                    reply.setOk(false);
                    reply.setException(e);
                    break;
                }
            }
        }
        deliveredList.clear();
        GenericRequest gr = new GenericRequest(-1, false, reply);
        ctx.sessionQueue.enqueue(gr);
    }

    protected void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/close");
        super.close();
        deliveredList.clear();
    }

    public String toString() {
        return "NontransactedSession, dispatchId=" + dispatchId;
    }
}

