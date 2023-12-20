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

import com.swiftmq.jms.smqp.v750.CloseSessionRequest;
import com.swiftmq.jms.smqp.v750.MessageDeliveredRequest;
import com.swiftmq.jms.smqp.v750.StartConsumerRequest;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.auth.AuthenticationSwiftlet;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.queue.QueueManager;
import com.swiftmq.swiftlet.queue.QueuePullTransaction;
import com.swiftmq.swiftlet.store.StoreSwiftlet;
import com.swiftmq.swiftlet.threadpool.EventLoop;
import com.swiftmq.swiftlet.threadpool.EventProcessor;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.topic.TopicManager;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;
import com.swiftmq.tools.collection.ExpandableList;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestService;

import javax.jms.InvalidDestinationException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Session extends SessionVisitor
        implements RequestService, EventProcessor {
    protected ExpandableList<Consumer> consumerList = new ExpandableList<>();
    protected ExpandableList<Producer> producerList = new ExpandableList<>();
    protected SessionContext ctx = null;
    protected int dispatchId;
    protected int recoveryEpoche = 0;
    protected boolean recoveryInProgress = false;
    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected JMSConnection myConnection = null;

    public Session(String connectionTracePrefix, Entity sessionEntity, EventLoop outboundLoop, int dispatchId, ActiveLogin activeLogin) {
        this.dispatchId = dispatchId;
        ctx = new SessionContext();
        ctx.queueManager = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
        ctx.topicManager = (TopicManager) SwiftletManager.getInstance().getSwiftlet("sys$topicmanager");
        ctx.authSwiftlet = (AuthenticationSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$authentication");
        ctx.threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
        ctx.storeSwiftlet = (StoreSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$store");
        ctx.logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
        ctx.traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        ctx.traceSpace = ctx.traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
        ctx.tracePrefix = connectionTracePrefix + "/" + this;
        ctx.activeLogin = activeLogin;
        ctx.sessionEntity = sessionEntity;
        ctx.sessionLoop = ctx.threadpoolSwiftlet.createEventLoop("sys$jms.session", this);
        ctx.outboundLoop = outboundLoop;
    }

    protected Session(String connectionTracePrefix, Entity sessionEntity, EventLoop outboundLoop, int dispatchId, ActiveLogin activeLogin, int ackMode) {
        this(connectionTracePrefix, sessionEntity, outboundLoop, dispatchId, activeLogin);
        ctx.ackMode = ackMode;
    }

    public JMSConnection getMyConnection() {
        return myConnection;
    }

    public void setMyConnection(JMSConnection myConnection) {
        this.myConnection = myConnection;
    }

    protected String validateDestination(String queueName) throws InvalidDestinationException {
        if (queueName.indexOf('@') == -1)
            return queueName + '@' + SwiftletManager.getInstance().getRouterName();
        if (!queueName.endsWith('@' + SwiftletManager.getInstance().getRouterName()))
            throw new InvalidDestinationException("Queue '" + queueName + "' is not local! Can't create a Consumer on it!");
        return queueName;
    }

    public void setRecoveryEpoche(int recoveryEpoche) {
        this.recoveryEpoche = recoveryEpoche;
    }

    @Override
    public void process(List<Object> events) {
        if (!closed.get())
            events.forEach(e -> ((Request) e).accept(this));
    }

    public void visit(StartConsumerRequest req) {
        if (closed.get())
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitStartConsumerRequest");
        int qcId = req.getQueueConsumerId();
        Consumer consumer = consumerList.get(qcId);
        if (consumer == null)
            return;
        int clientDispatchId = req.getClientDispatchId();
        int clientListenerId = req.getClientListenerId();
        try {
            AsyncMessageProcessor mp = (AsyncMessageProcessor) consumer.getMessageProcessor();
            if (mp == null) {
                mp = new AsyncMessageProcessor(this, ctx, consumer, req.getConsumerCacheSize(), recoveryEpoche);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitStartConsumerRequest, new message processor: " + mp);
                consumer.setMessageListener(clientDispatchId, clientListenerId, mp);
            }
            mp.setMaxBulkSize(req.getConsumerCacheSizeKB());
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitStartConsumerRequest, register message processor: " + mp);
            if (!mp.isStarted())
                mp.register();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void visit(DeliveryItem item) {
        if (closed.get() || recoveryInProgress || item.request.getRecoveryEpoche() != recoveryEpoche)
            return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitDeliveryItem, item= " + item);
        try {
            item.request.setMessageEntry(item.messageEntry);
            ctx.outboundLoop.submit(item.request);
        } catch (Exception e) {
            if (!closed.get()) {
                e.printStackTrace();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/handleDelivery, exception= " + e);
            }
        }
    }

    public void visit(RegisterMessageProcessor request) {
        if (closed.get())
            return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitRegisterMessageProcessor, request= " + request);
        request.getMessageProcessor().register();
    }

    public void visit(RunMessageProcessor request) {
        if (closed.get())
            return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitRunMessageProcessor, request= " + request);
        request.getMessageProcessor().run();
    }

    protected boolean isCountDeliveredRequests() {
        return false;
    }

    public void visit(MessageDeliveredRequest req) {
        if (closed.get() || recoveryInProgress)
            return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitMessageDeliveredRequest ...");
        try {
            Consumer consumer = consumerList.get(req.getQueueConsumerId());
            QueuePullTransaction rt = consumer.getReadTransaction();
            // Duplicates are immediately deleted
            if (req.isDuplicate()) {
                QueuePullTransaction t = (QueuePullTransaction) consumer.createDuplicateTransaction();
                t.moveToTransaction(req.getMessageIndex(), rt);
                t.commit();
            } else {
                QueuePullTransaction t = consumer.getTransaction();
                long size = t.moveToTransactionReturnSize(req.getMessageIndex(), rt);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitMessageDeliveredRequest, isCountDeliveredDequests()=" + isCountDeliveredRequests());
                if (isCountDeliveredRequests()) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitMessageDeliveredRequest, comnsumer=" + consumer + ", size=" + size);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitMessageDeliveredRequest, exception=" + e);
        }
    }

    public void visit(CloseSessionRequest request) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCloseSessionRequest...");
        close();
        request._sem.notifySingleWaiter();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCloseSessionRequest...DONE");
    }

    public void serviceRequest(Request request) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/serviceRequest, request=" + request);
        ctx.sessionLoop.submit(request);
    }

    protected void close() {
        if (closed.getAndSet(true))
            return;
        ctx.sessionLoop.close();

        for (int i = 0; i < consumerList.size(); i++) {
            Consumer consumer = consumerList.get(i);
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (Exception e) {
                }
                ctx.activeLogin.getResourceLimitGroup().decConsumers();
            }
        }
        for (int i = 0; i < producerList.size(); i++) {
            Producer producer = producerList.get(i);
            if (producer != null) {
                try {
                    producer.close();
                } catch (Exception e) {
                }
                ctx.activeLogin.getResourceLimitGroup().decProducers();
            }
        }
    }

    protected boolean isClosed() {
        return closed.get();
    }

    public String toString() {
        return "Session, dispatchId=" + dispatchId;
    }

}

