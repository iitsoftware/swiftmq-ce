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

package com.swiftmq.jms.v600;

import com.swiftmq.client.thread.PoolManager;
import com.swiftmq.jms.ExceptionConverter;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.smqp.v600.AsyncMessageDeliveryRequest;
import com.swiftmq.jms.smqp.v600.CloseSessionRequest;
import com.swiftmq.jms.smqp.v600.StartConsumerRequest;
import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.tools.queue.SingleProcessorQueue;
import com.swiftmq.tools.requestreply.*;
import com.swiftmq.tools.tracking.MessageTracker;
import com.swiftmq.tools.util.IdGenerator;

import javax.jms.ConnectionConsumer;
import javax.jms.JMSException;
import javax.jms.ServerSession;
import javax.jms.ServerSessionPool;

public abstract class ConnectionConsumerImpl
        implements ConnectionConsumer, RequestService, Recreatable, RequestRetryValidator {
    public static final String DISPATCH_TOKEN = "sys$jms.client.session.connectionconsumer.queuetask";

    String uniqueConsumerId = IdGenerator.getInstance().nextId('/');
    ConnectionImpl myConnection = null;
    int dispatchId = 0;
    int myDispatchId = 0;
    RequestRegistry requestRegistry = null;
    ThreadPool sessionPool = null;
    DeliveryQueue deliveryQueue = null;
    QueueTask queueTask = null;
    ServerSessionPool serverSessionPool;
    int maxMessages = 0;
    ServerSession currentServerSession = null;
    SessionImpl currentSession = null;
    int nCurrent = 0;
    boolean closed = false;
    volatile boolean resetInProgress = false;

    public ConnectionConsumerImpl(ConnectionImpl myConnection, int dispatchId, RequestRegistry requestRegistry, ServerSessionPool serverSessionPool, int maxMessages) {
        this.myConnection = myConnection;
        this.dispatchId = dispatchId;
        this.requestRegistry = requestRegistry;
        this.serverSessionPool = serverSessionPool;
        this.maxMessages = maxMessages;
        this.sessionPool = PoolManager.getInstance().getSessionPool();
        queueTask = new QueueTask();
        deliveryQueue = new DeliveryQueue();
    }

    public void setResetInProgress(boolean resetInProgress) {
        this.resetInProgress = resetInProgress;
        if (resetInProgress) {
            deliveryQueue.stopQueue();
            deliveryQueue.clear();
            deliveryQueue.setCurrentCallInvalid(true);
        } else {
            deliveryQueue.clear();
            deliveryQueue.startQueue();
            fillCache();
        }
    }

    public void validate(Request request) throws ValidationException {
        request.setDispatchId(dispatchId);
    }

    void startConsumer() {
        deliveryQueue.startQueue();
    }

    void stopConsumer() {
        deliveryQueue.stopQueue();
    }

    void fillCache() {
        requestRegistry.request(new StartConsumerRequest(this, dispatchId, 0, myDispatchId, 0, myConnection.getSmqpConsumerCacheSize()));
    }

    protected abstract String getQueueName();

    public void setMyDispatchId(int myDispatchId) {
        this.myDispatchId = myDispatchId;
    }

    int getMsgNo(AsyncMessageDeliveryRequest request) {
        int n = 0;
        try {
            n = request.getMessageEntry().getMessage().getIntProperty("no");
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return n;
    }

    public void serviceRequest(Request request) {
        if (MessageTracker.enabled) {
            MessageTracker.getInstance().track(((AsyncMessageDeliveryRequest) request).getMessageEntry().getMessage(), new String[]{myConnection.toString(), toString()}, "serviceRequest");
        }
        deliveryQueue.enqueue(request);
    }

    public boolean isDuplicate(MessageImpl msg) {
        if (!myConnection.isDuplicateMessageDetection())
            return false;
        return myConnection.isDuplicate(SessionImpl.buildId(uniqueConsumerId, msg));
    }

    public void processRequest(AsyncMessageDeliveryRequest request, boolean hasNext) {
        if (resetInProgress || deliveryQueue.isCurrentCallInvalid()) {
            if (MessageTracker.enabled) {
                MessageTracker.getInstance().track(((AsyncMessageDeliveryRequest) request).getMessageEntry().getMessage(), new String[]{myConnection.toString(), toString()}, "processRequest, resetInProgress || currentCallInvalid, return");
            }
            return;
        }
        if (request.getConnectionId() != myConnection.getConnectionId()) {
            if (MessageTracker.enabled) {
                MessageTracker.getInstance().track(((AsyncMessageDeliveryRequest) request).getMessageEntry().getMessage(), new String[]{myConnection.toString(), toString()}, "processRequest, invalid connectionId (" + request.getConnectionId() + " vs " + myConnection.getConnectionId() + ")");
            }
            return;
        }
        try {
            if (currentServerSession == null) {
                currentServerSession = serverSessionPool.getServerSession();
                if (currentServerSession.getSession() instanceof XASessionImpl)
                    currentSession = ((XASessionImpl) currentServerSession.getSession()).session;
                else
                    currentSession = (SessionImpl) currentServerSession.getSession();
                nCurrent = 0;
                if (!currentSession.isShadowConsumerCreated())
                    currentSession.createShadowConsumer(getQueueName());
                currentSession.setConnectionConsumer(this);
                if (resetInProgress || deliveryQueue.isCurrentCallInvalid()) {
                    if (MessageTracker.enabled) {
                        MessageTracker.getInstance().track(((AsyncMessageDeliveryRequest) request).getMessageEntry().getMessage(), new String[]{myConnection.toString(), toString()}, "processRequest, resetInProgress || currentCallInvalid, return (2)");
                    }
                    return;
                }
            }
            if (MessageTracker.enabled) {
                MessageTracker.getInstance().track(((AsyncMessageDeliveryRequest) request).getMessageEntry().getMessage(), new String[]{myConnection.toString(), toString()}, "processRequest, " + request.getConnectionId() + " / " + myConnection.getConnectionId());
            }
            MessageEntry me = request.getMessageEntry();
            me.setConnectionId(request.getConnectionId());
            currentSession.addMessageChunk(me);
            nCurrent++;
            if (nCurrent == maxMessages || !hasNext) {
                currentServerSession.start();
                currentServerSession = null;
                currentSession = null;
                nCurrent = 0;
            }
        } catch (Exception e) {
            e.printStackTrace();
            // TODO: Handle it some way like InvalidateConsumerRequest!!!
        }
        if (request.isRequiresRestart())
            fillCache();
    }

    public ServerSessionPool getServerSessionPool() throws JMSException {
        return serverSessionPool;
    }

    public boolean isClosed() {
        return closed;
    }

    public void close() throws JMSException {
        if (closed)
            return;
        Reply reply = null;
        try {
            reply = requestRegistry.request(new CloseSessionRequest(this, 0, dispatchId));
        } catch (Exception e) {
            throw ExceptionConverter.convert(e);
        }
        deliveryQueue.stopQueue();
        deliveryQueue.close();
        myConnection.removeRequestService(myDispatchId);
        myConnection.removeConnectionConsumer(this);
        if (!reply.isOk()) {
            throw ExceptionConverter.convert(reply.getException());
        }
    }

    void cancel() {
        closed = true;
        deliveryQueue.stopQueue();
        deliveryQueue.close();
    }

    private class DeliveryQueue extends SingleProcessorQueue {
        boolean currentCallInvalid = false;

        public DeliveryQueue() {
            super(myConnection.smqpConsumerCacheSize);
        }

        public void setCurrentCallInvalid(boolean currentCallInvalid) {
            this.currentCallInvalid = currentCallInvalid;
        }

        public boolean isCurrentCallInvalid() {
            return currentCallInvalid;
        }

        protected void startProcessor() {
            if (!closed)
                sessionPool.dispatchTask(queueTask);
        }

        private boolean valid() {
            return !resetInProgress && isStarted();
        }

        protected void process(Object[] bulk, int n) {
            // This flag needs only to be respected when returning from processRequest!
            if (currentCallInvalid)
                currentCallInvalid = false;
            for (int i = 0; i < n; i++) {
                processRequest((AsyncMessageDeliveryRequest) bulk[i], i + 1 < n);
                if (!valid() || currentCallInvalid)
                    return;
            }
        }
    }

    private class QueueTask implements AsyncTask {
        public boolean isValid() {
            return !closed;
        }

        public String getDispatchToken() {
            return DISPATCH_TOKEN;
        }

        public String getDescription() {
            return myConnection.myHostname + "/ConnectionConsumer/QueueTask";
        }

        public void run() {
            if (!closed && deliveryQueue.dequeue())
                sessionPool.dispatchTask(this);
        }

        public void stop() {
        }
    }
}
