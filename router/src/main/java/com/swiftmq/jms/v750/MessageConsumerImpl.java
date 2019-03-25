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

package com.swiftmq.jms.v750;

import com.swiftmq.jms.ExceptionConverter;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.SwiftMQMessageConsumer;
import com.swiftmq.jms.smqp.v750.*;
import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.swiftlet.queue.MessageIndex;
import com.swiftmq.tools.collection.RingBuffer;
import com.swiftmq.tools.collection.RingBufferThreadsafe;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.requestreply.*;
import com.swiftmq.tools.tracking.MessageTracker;
import com.swiftmq.tools.util.IdGenerator;
import com.swiftmq.tools.util.UninterruptableWaiter;

import javax.jms.IllegalStateException;
import javax.jms.*;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MessageConsumerImpl implements MessageConsumer, SwiftMQMessageConsumer, Recreatable, RequestRetryValidator {
    String uniqueConsumerId = IdGenerator.getInstance().nextId('/');
    boolean closed = false;
    volatile int consumerId = 0;
    boolean transacted = false;
    int acknowledgeMode = 0;
    RequestRegistry requestRegistry = null;
    String messageSelector = null;
    MessageListener messageListener = null;
    SessionImpl mySession = null;
    int serverQueueConsumerId = -1;
    boolean useThreadContextCL = false;
    boolean cancelled = false;
    RingBuffer messageCache = null;
    boolean doAck = false;
    boolean reportDelivered = false;
    boolean recordLog = true;
    boolean receiverWaiting = false;
    boolean wasRecovered = false;
    volatile boolean fillCachePending = false;
    boolean receiveNoWaitFirstCall = true;
    boolean consumerStarted = false;
    Lock fillCacheLock = new ReentrantLock();

    public MessageConsumerImpl(boolean transacted, int acknowledgeMode, RequestRegistry requestRegistry,
                               String messageSelector, SessionImpl session) {
        this.transacted = transacted;
        this.acknowledgeMode = acknowledgeMode;
        this.requestRegistry = requestRegistry;
        this.messageSelector = messageSelector;
        this.mySession = session;
        useThreadContextCL = mySession.getMyConnection().isUseThreadContextCL();
        reportDelivered = transacted || acknowledgeMode == Session.CLIENT_ACKNOWLEDGE;
        messageCache = new RingBufferThreadsafe(mySession.getMyConnection().getSmqpConsumerCacheSize());
    }

    public Request getRecreateRequest() {
        return null;
    }

    public void setRecreateReply(Reply reply) {

    }

    public List getRecreatables() {
        return null;
    }

    public void validate(Request request) throws ValidationException {
        request.setDispatchId(mySession.dispatchId);
        if (request instanceof CloseConsumerRequest) {
            CloseConsumerRequest r = (CloseConsumerRequest) request;
            r.setSessionDispatchId(mySession.dispatchId);
            r.setQueueConsumerId(serverQueueConsumerId);
        } else {
            request.setCancelledByValidator(true);
        }
    }

    protected void verifyState() throws JMSException {
        if (closed) {
            throw new javax.jms.IllegalStateException("Message consumer is closed");
        }

        mySession.verifyState();
    }

    public boolean isConsumerStarted() {
        return consumerStarted;
    }

    void setWasRecovered(boolean wasRecovered) {
        this.wasRecovered = wasRecovered;
    }

    void setDoAck(boolean doAck) {
        this.doAck = doAck;
    }

    public void setRecordLog(boolean recordLog) {
        this.recordLog = recordLog;
    }

    void addToCache(AsyncMessageDeliveryRequest request) {
        if (isClosed())
            return;
        if (request.isRequiresRestart())
            fillCachePending = false;
        MessageImpl msg = request.getMessageEntry().getMessage();
        if (request.getConnectionId() != mySession.myConnection.getConnectionId()) {
            if (MessageTracker.enabled) {
                MessageTracker.getInstance().track(((AsyncMessageDeliveryRequest) request).getMessageEntry().getMessage(), new String[]{mySession.myConnection.toString(), mySession.toString(), toString()}, "addToCache, invalid connectionId (" + request.getConnectionId() + " vs " + mySession.myConnection.getConnectionId() + ")");
            }
            return;
        }
        if (MessageTracker.enabled) {
            MessageTracker.getInstance().track(msg, new String[]{mySession.myConnection.toString(), mySession.toString(), toString()}, "addToCache");
        }
        messageCache.add(request);
    }

    void addToCache(AsyncMessageDeliveryRequest[] requests, boolean lastRestartRequired) {
        for (int i = 0; i < requests.length; i++) {
            if (lastRestartRequired && i == requests.length - 1)
                requests[i].setRequiresRestart(true);
            addToCache(requests[i]);
        }
    }

    synchronized boolean invokeConsumer() {
        if (messageCache.getSize() > 0) {
            if (messageListener == null) {
                if (receiverWaiting) {
                    receiverWaiting = false;
                    notify();
                }
            } else
                invokeMessageListener();
        }
        return messageCache.getSize() > 0 && (messageListener != null || receiverWaiting) && !isClosed();
    }

    void fillCache(boolean force) {
        fillCacheLock.lock();
        try {
            if (isClosed() || fillCachePending && !force)
                return;
            fillCachePending = true;
            consumerStarted = true;
            requestRegistry.request(new StartConsumerRequest(this, mySession.dispatchId, serverQueueConsumerId,
                    mySession.getMyDispatchId(), consumerId, mySession.getMyConnection().getSmqpConsumerCacheSize(), mySession.getMyConnection().getSmqpConsumerCacheSizeKB()));
        } finally {
            fillCacheLock.unlock();
        }
    }

    void fillCache() {
        fillCache(false);
    }

    void clearCache() {
        fillCachePending = false;
        messageCache.clear();
    }

    public boolean isClosed() {
        return closed || mySession.isClosed();
    }

    void setConsumerId(int id) {
        consumerId = id;
    }

    int getConsumerId() {
        return consumerId;
    }

    void setServerQueueConsumerId(int id) {
        serverQueueConsumerId = id;
    }

    int getServerQueueConsumerId() {
        return serverQueueConsumerId;
    }

    public String getMessageSelector() throws JMSException {
        verifyState();

        return messageSelector;
    }

    public synchronized MessageListener getMessageListener() throws JMSException {
        verifyState();

        return (messageListener);
    }

    public synchronized void setMessageListener(MessageListener listener) throws JMSException {
        verifyState();

        if (listener != null && !consumerStarted)
            fillCache();
        messageListener = listener;
        if (listener != null)
            mySession.triggerInvocation();
    }

    private void invokeMessageListener() {
        if (isClosed())
            return;
        AsyncMessageDeliveryRequest request = (AsyncMessageDeliveryRequest) messageCache.remove();
        if (request.getConnectionId() != mySession.myConnection.getConnectionId()) {
            if (MessageTracker.enabled) {
                MessageTracker.getInstance().track(request.getMessageEntry().getMessage(), new String[]{mySession.myConnection.toString(), mySession.toString(), toString()}, "invokeMessageListener, invalid connectionId (" + request.getConnectionId() + " vs " + mySession.myConnection.getConnectionId() + ")");
            }
            return;
        }
        MessageEntry messageEntry = request.getMessageEntry();
        MessageImpl msg = messageEntry.getMessage();
        messageEntry.moveMessageAttributes();
        MessageIndex msgIndex = msg.getMessageIndex();
        msg.setMessageConsumerImpl(this);
        try {
            msg.reset();
        } catch (JMSException e) {
            e.printStackTrace();
        }
        msg.setReadOnly(true);
        msg.setUseThreadContextCL(useThreadContextCL);
        String id = null;
        boolean duplicate = false;
        if (recordLog) {
            id = SessionImpl.buildId(uniqueConsumerId, msg);
            duplicate = mySession.myConnection.isDuplicateMessageDetection() && mySession.isDuplicate(id);
        }
        if (MessageTracker.enabled) {
            MessageTracker.getInstance().track(msg, new String[]{mySession.myConnection.toString(), mySession.toString(), toString()}, "invokeMessageListener, duplicate=" + duplicate);
        }
        if (reportDelivered)
            reportDelivered(msg, false);
        try {
            if (!duplicate) {
                if (recordLog && mySession.myConnection.isDuplicateMessageDetection())
                    mySession.addCurrentTxLog(id);
                if (MessageTracker.enabled) {
                    MessageTracker.getInstance().track(msg, new String[]{mySession.myConnection.toString(), mySession.toString(), toString()}, "invokeMessageListener, onMessage...");
                }
                mySession.withinOnMessage = true;
                mySession.onMessageMessage = msg;
                mySession.onMessageConsumer = this;
                mySession.setTxCancelled(false);
                messageListener.onMessage(msg);
                mySession.onMessageMessage = null;
                mySession.onMessageConsumer = null;
                mySession.withinOnMessage = false;
                if (MessageTracker.enabled) {
                    MessageTracker.getInstance().track(msg, new String[]{mySession.myConnection.toString(), mySession.toString(), toString()}, "invokeMessageListener, onMessage ok");
                }
                if (mySession.isTxCancelled() || mySession.acknowledgeMode == Session.CLIENT_ACKNOWLEDGE && msg.isCancelled()) {
                    if (MessageTracker.enabled) {
                        MessageTracker.getInstance().track(msg, new String[]{mySession.myConnection.toString(), mySession.toString(), toString()}, "tx was cancelled, return!");
                    }
                    wasRecovered = false;
                    return;
                }
            }
        } catch (RuntimeException e) {
            if (MessageTracker.enabled) {
                MessageTracker.getInstance().track(msg, new String[]{mySession.myConnection.toString(), mySession.toString(), toString()}, "invokeMessageListener, exception=" + e);
            }
            System.err.println("ERROR! MessageListener throws RuntimeException, shutting down consumer!");
            e.printStackTrace();
            try {
                close(e.toString());
            } catch (JMSException e1) {
            }
            return;
        }
        if (!wasRecovered) {
            if (request.isRequiresRestart())
                fillCache();
            if (doAck) {
                try {
                    if (MessageTracker.enabled) {
                        MessageTracker.getInstance().track(msg, new String[]{mySession.myConnection.toString(), mySession.toString(), toString()}, "invokeMessageListener, ack");
                    }
                    boolean cancelled = acknowledgeMessage(msgIndex, false);
                    if (MessageTracker.enabled) {
                        MessageTracker.getInstance().track(msg, new String[]{mySession.myConnection.toString(), mySession.toString(), toString()}, "invokeMessageListener, ack, cancelled=" + cancelled);
                    }
                } catch (JMSException e) {
                }
            }
        } else
            wasRecovered = false;
    }

    protected void reportDelivered(Message message, boolean duplicate) {
        try {
            MessageIndex messageIndex = ((MessageImpl) message).getMessageIndex();
            requestRegistry.request(new MessageDeliveredRequest(this, mySession.dispatchId, serverQueueConsumerId, messageIndex, duplicate));
        } catch (Exception e) {
        }
    }

    public boolean acknowledgeMessage(MessageImpl message) throws JMSException {
        if (transacted)
            throw new IllegalStateException("acknowledge not possible, session is transacted!");
        if (!(acknowledgeMode == Session.CLIENT_ACKNOWLEDGE))
            throw new IllegalStateException("acknowledge not possible, session was not created in mode CLIENT_ACKNOWLEDGE!");
        return acknowledgeMessage(message.getMessageIndex(), true);
    }

    private boolean acknowledgeMessage(MessageIndex messageIndex, boolean replyRequired) throws JMSException {
        if (isClosed())
            throw new javax.jms.IllegalStateException("Connection is closed");

        Reply reply = null;
        boolean cancelled = false;
        try {
            if (messageIndex == null)
                throw new JMSException("Unable to acknowledge message - missing message key!");

            AcknowledgeMessageRequest request = new AcknowledgeMessageRequest(this, mySession.dispatchId, serverQueueConsumerId, messageIndex);
            request.setReplyRequired(replyRequired);
            reply = requestRegistry.request(request);
            if (request.isCancelledByValidator()) {
                cancelled = true;
                mySession.addCurrentTxToDuplicateLog();
            }
            mySession.removeCurrentTxFromRollbackLog();
            mySession.clearCurrentTxLog();
        } catch (Exception e) {
            if (isClosed()) throw new javax.jms.IllegalStateException("Connection is closed: " + e);
            throw ExceptionConverter.convert(e);
        }

        if (replyRequired && !reply.isOk()) {
            if (isClosed()) throw new javax.jms.IllegalStateException("Connection is closed: " + reply.getException());
            throw ExceptionConverter.convert(reply.getException());
        }
        return cancelled;
    }

    synchronized Message receiveMessage(boolean block, long timeout) throws JMSException {
        verifyState();

        if (messageListener != null) {
            throw new JMSException("receive not allowed while a message listener has been set");
        }
        boolean wasDuplicate = false;
        boolean wasInvalidConnectionId = false;
        MessageImpl msg = null;
        String id = null;
        do {
            wasDuplicate = false;
            wasInvalidConnectionId = false;
            if (!consumerStarted)
                fillCache();
            do {
                if (messageCache.getSize() == 0) {
                    if (block) {
                        receiverWaiting = true;
                        if (timeout == 0) {
                            UninterruptableWaiter.doWait(this);
                        } else {
                            long to = timeout;
                            do {
                                long startWait = System.currentTimeMillis();
                                UninterruptableWaiter.doWait(this, to);
                                long delta = System.currentTimeMillis() - startWait;
                                to -= delta;
                            }
                            while (to > 0 && messageCache.getSize() == 0 && fillCachePending && !cancelled && !isClosed());
                        }
                    } else {
                        if (fillCachePending && receiveNoWaitFirstCall) {
                            receiverWaiting = true;
                            UninterruptableWaiter.doWait(this, 1000);
                        }
                    }
                    if (cancelled)
                        return null;
                }
            } while (mySession.resetInProgress);
            receiverWaiting = false;
            if (messageCache.getSize() == 0 || isClosed())
                return null;

            AsyncMessageDeliveryRequest request = (AsyncMessageDeliveryRequest) messageCache.remove();
            if (request.getConnectionId() != mySession.myConnection.getConnectionId()) {
                if (MessageTracker.enabled) {
                    MessageTracker.getInstance().track(request.getMessageEntry().getMessage(), new String[]{mySession.myConnection.toString(), mySession.toString(), toString()}, "receiveMessage, invalid connectionId (" + request.getConnectionId() + " vs " + mySession.myConnection.getConnectionId() + ")");
                }
                wasInvalidConnectionId = true;
            } else {
                MessageEntry messageEntry = request.getMessageEntry();
                msg = messageEntry.getMessage();
                messageEntry.moveMessageAttributes();
                msg.setMessageConsumerImpl(this);
                msg.reset();
                msg.setReadOnly(true);
                msg.setUseThreadContextCL(useThreadContextCL);
                if (request.isRequiresRestart())
                    fillCache();
                if (recordLog) {
                    id = SessionImpl.buildId(uniqueConsumerId, msg);
                    wasDuplicate = mySession.myConnection.isDuplicateMessageDetection() && mySession.isDuplicate(id);
                }
                if (MessageTracker.enabled) {
                    MessageTracker.getInstance().track(msg, new String[]{mySession.myConnection.toString(), mySession.toString(), toString()}, "receivedMessage, duplicate=" + wasDuplicate);
                }
                if (reportDelivered)
                    reportDelivered(msg, false);
                if (doAck) {
                    try {
                        if (MessageTracker.enabled) {
                            MessageTracker.getInstance().track(msg, new String[]{mySession.myConnection.toString(), mySession.toString(), toString()}, "receivedMessage, ack...");
                        }
                        boolean cancelled = acknowledgeMessage(msg.getMessageIndex(), false);
                        if (MessageTracker.enabled) {
                            MessageTracker.getInstance().track(msg, new String[]{mySession.myConnection.toString(), mySession.toString(), toString()}, "receivedMessage, ack, cancelled=" + cancelled);
                        }
                    } catch (JMSException e) {
                    }
                }
                if (wasDuplicate) {
                    msg = null;
                }
            }
        } while (wasDuplicate || wasInvalidConnectionId);

        if (recordLog && mySession.myConnection.isDuplicateMessageDetection())
            mySession.addCurrentTxLog(id);
        return msg;
    }

    public Message receive() throws JMSException {
        return receiveMessage(true, 0);
    }

    public Message receive(long timeOut) throws JMSException {
        return receiveMessage(true, timeOut);
    }

    public Message receiveNoWait() throws JMSException {
        Message msg = receiveMessage(false, 0);
        receiveNoWaitFirstCall = false;
        return msg;
    }

    void close(String exception) throws JMSException {
        synchronized (this) {
            if (isClosed())
                return;
            closed = true;
            messageCache.clear();
            notify();
        }

        Reply reply = null;

        // must be released by the connection!
        try {
            reply = requestRegistry.request(new CloseConsumerRequest(this, mySession.dispatchId, mySession.dispatchId, serverQueueConsumerId, exception));
        } catch (Exception e) {
            throw ExceptionConverter.convert(e);
        }

        if (!reply.isOk()) {
            throw ExceptionConverter.convert(reply.getException());
        }
        mySession.removeMessageConsumerImpl(this);
    }

    public void close() throws JMSException {
        if (closed)
            return;
        if (!mySession.isSessionStarted()) {
            close(null);
            return;
        }
        CloseConsumer request = new CloseConsumer(consumerId);
        request._sem = new Semaphore();
        mySession.serviceRequest(request);
        request._sem.waitHere();
    }

    void cancel() {
        synchronized (this) {
            cancelled = true;
            closed = true;
            messageCache.clear();
            notify();
        }
    }

}



