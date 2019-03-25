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

package com.swiftmq.jms.v500;

import com.swiftmq.jms.ExceptionConverter;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.SwiftMQMessageConsumer;
import com.swiftmq.jms.smqp.v500.*;
import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.swiftlet.queue.MessageIndex;
import com.swiftmq.tools.collection.RingBuffer;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.RequestRegistry;

import javax.jms.*;

public class MessageConsumerImpl implements MessageConsumer, SwiftMQMessageConsumer {
    boolean closed = false;
    int consumerId = 0;
    boolean transacted = false;
    int acknowledgeMode = 0;
    RequestRegistry requestRegistry = null;
    int dispatchId = 0;
    String messageSelector = null;
    MessageListener messageListener = null;
    SessionImpl mySession = null;
    int serverQueueConsumerId = -1;
    boolean useThreadContextCL = false;
    boolean cancelled = false;
    RingBuffer messageCache = null;
    boolean doAck = false;
    boolean reportDelivered = false;
    boolean receiverWaiting = false;
    boolean wasRecovered = false;
    boolean fillCachePending = false;
    boolean receiveNoWaitFirstCall = true;
    boolean consumerStarted = false;

    public MessageConsumerImpl(boolean transacted, int acknowledgeMode,
                               int dispatchId, RequestRegistry requestRegistry,
                               String messageSelector, SessionImpl session) {
        this.transacted = transacted;
        this.acknowledgeMode = acknowledgeMode;
        this.dispatchId = dispatchId;
        this.requestRegistry = requestRegistry;
        this.messageSelector = messageSelector;
        this.mySession = session;
        useThreadContextCL = mySession.getMyConnection().isUseThreadContextCL();
        reportDelivered = transacted || !transacted && acknowledgeMode == Session.CLIENT_ACKNOWLEDGE;
        messageCache = new RingBuffer(mySession.getMyConnection().getSmqpConsumerCacheSize());
    }

    protected void verifyState() throws JMSException {
        if (closed) {
            throw new javax.jms.IllegalStateException("Message consumer is closed");
        }

        mySession.verifyState();
    }

    void setWasRecovered(boolean wasRecovered) {
        this.wasRecovered = wasRecovered;
    }

    void setDoAck(boolean doAck) {
        this.doAck = doAck;
    }

    synchronized void addToCache(AsyncMessageDeliveryRequest request) {
        messageCache.add(request);
    }

    synchronized void addToCache(AsyncMessageDeliveryRequest[] requests, boolean lastRestartRequired) {
        fillCachePending = false;
        for (int i = 0; i < requests.length; i++) {
            if (lastRestartRequired && i == requests.length - 1)
                requests[i].setRequiresRestart(true);
            messageCache.add(requests[i]);
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
        boolean rc = messageCache.getSize() > 0 && (messageListener != null || receiverWaiting) && !isClosed();
        return rc;
    }

    void fillCache() {
        fillCachePending = true;
        consumerStarted = true;
        requestRegistry.request(new StartConsumerRequest(dispatchId,
                serverQueueConsumerId,
                mySession.getMyDispatchId(),
                consumerId,
                mySession.getMyConnection().getSmqpConsumerCacheSize()));
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
        if (reportDelivered)
            reportDelivered(msg);
        try {
            messageListener.onMessage(msg);
        } catch (RuntimeException e) {
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
                    acknowledgeMessage(msgIndex, false);
                } catch (JMSException e) {
                }
            }
        } else
            wasRecovered = false;
    }

    private void reportDelivered(Message message) {
        try {
            MessageIndex messageIndex = ((MessageImpl) message).getMessageIndex();
            requestRegistry.request(new MessageDeliveredRequest(dispatchId, serverQueueConsumerId, messageIndex));
        } catch (Exception e) {
        }
    }

    public boolean acknowledgeMessage(MessageImpl message) throws JMSException {
        acknowledgeMessage(message.getMessageIndex(), true);
        return false;
    }

    private void acknowledgeMessage(MessageIndex messageIndex, boolean replyRequired) throws JMSException {
        if (isClosed())
            throw new javax.jms.IllegalStateException("Connection is closed");

        Reply reply = null;

        try {
            if (messageIndex == null)
                throw new JMSException("Unable to acknowledge message - missing message key!");

            reply = requestRegistry.request(new AcknowledgeMessageRequest(dispatchId, serverQueueConsumerId, messageIndex, replyRequired));
        } catch (Exception e) {
            if (isClosed()) throw new javax.jms.IllegalStateException("Connection is closed: " + e);
            throw ExceptionConverter.convert(e);
        }

        if (replyRequired && !reply.isOk()) {
            if (isClosed()) throw new javax.jms.IllegalStateException("Connection is closed: " + reply.getException());
            throw ExceptionConverter.convert(reply.getException());
        }
    }

    synchronized Message receiveMessage(boolean block, long timeout) throws JMSException {
        verifyState();

        if (messageListener != null) {
            throw new JMSException("receive not allowed while a message listener has been set");
        }
        try {
            if (!consumerStarted)
                fillCache();
            if (messageCache.getSize() == 0) {
                if (block) {
                    receiverWaiting = true;
                    if (timeout == 0)
                        wait();
                    else
                        wait(timeout);
                } else {
                    if (fillCachePending && receiveNoWaitFirstCall) {
                        wait(1000);
                    }
                }
            }
        } catch (InterruptedException e) {
        }
        receiverWaiting = false;
        if (messageCache.getSize() == 0 || isClosed())
            return null;

        AsyncMessageDeliveryRequest request = (AsyncMessageDeliveryRequest) messageCache.remove();
        MessageEntry messageEntry = request.getMessageEntry();
        MessageImpl msg = messageEntry.getMessage();
        messageEntry.moveMessageAttributes();
        msg.setMessageConsumerImpl(this);
        msg.reset();
        msg.setReadOnly(true);
        msg.setUseThreadContextCL(useThreadContextCL);
        if (request.isRequiresRestart())
            fillCache();
        if (reportDelivered)
            reportDelivered(msg);
        if (doAck) {
            try {
                acknowledgeMessage(msg.getMessageIndex(), false);
            } catch (JMSException e) {
            }
        }

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

    private void close(String exception) throws JMSException {
        synchronized (this) {
            messageCache.clear();
            notify();
        }
        if (isClosed())
            return;
        closed = true;

        synchronized (mySession) {
            Reply reply = null;

            // must be released by the connection!
            try {
                reply = requestRegistry.request(new CloseConsumerRequest(dispatchId, dispatchId, serverQueueConsumerId, exception));
            } catch (Exception e) {
                throw ExceptionConverter.convert(e);
            }

            if (!reply.isOk()) {
                throw ExceptionConverter.convert(reply.getException());
            }
        }
        mySession.removeMessageConsumerImpl(this);
    }

    public void close() throws JMSException {
        close(null);
    }

    void cancel() {
        cancelled = true;
        closed = true;
        synchronized (this) {
            messageCache.clear();
            notify();
        }
    }

}



