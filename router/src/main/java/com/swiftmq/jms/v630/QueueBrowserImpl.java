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

package com.swiftmq.jms.v630;

import com.swiftmq.jms.ExceptionConverter;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.smqp.v630.*;
import com.swiftmq.swiftlet.queue.MessageIndex;
import com.swiftmq.tools.requestreply.*;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;

public class QueueBrowserImpl implements QueueBrowser, Enumeration, Recreatable, RequestRetryValidator {
    boolean closed = false;
    Queue queue = null;
    String messageSelector = null;
    int dispatchId = -1;
    int browserDispatchId = -1;
    RequestRegistry requestRegistry = null;
    Message lastMessage = null;
    MessageIndex lastMessageIndex = null;
    SessionImpl mySession = null;
    boolean resetRequired = false;

    public QueueBrowserImpl(SessionImpl mySession, Queue queue,
                            String messageSelector, int dispatchId,
                            int browserDispatchId,
                            RequestRegistry requestRegistry) {
        this.mySession = mySession;
        this.queue = queue;
        this.messageSelector = messageSelector;
        this.dispatchId = dispatchId;
        this.browserDispatchId = browserDispatchId;
        this.requestRegistry = requestRegistry;
    }

    public Request getRecreateRequest() {
        return new CreateBrowserRequest(mySession, mySession.dispatchId, (QueueImpl) queue, messageSelector);
    }

    public void setRecreateReply(Reply reply) {
        CreateBrowserReply r = (CreateBrowserReply) reply;
        dispatchId = mySession.dispatchId;
        browserDispatchId = r.getQueueBrowserId();
    }

    public List getRecreatables() {
        return null;
    }

    public void validate(Request request) throws ValidationException {
        request.setDispatchId(dispatchId);
        if (request instanceof CloseBrowserRequest) {
            CloseBrowserRequest r = (CloseBrowserRequest) request;
            r.setQueueBrowserId(browserDispatchId);
        } else if (request instanceof FetchBrowserMessageRequest) {
            FetchBrowserMessageRequest r = (FetchBrowserMessageRequest) request;
            r.setQueueBrowserId(browserDispatchId);
        }
    }

    protected void verifyState() throws JMSException {
        if (closed) {
            throw new JMSException("Queue browser is closed");
        }

        mySession.verifyState();
    }

    public Queue getQueue() throws JMSException {
        verifyState();

        return (queue);
    }

    public String getMessageSelector() throws JMSException {
        verifyState();

        return messageSelector;
    }

    public Enumeration getEnumeration() throws JMSException {
        verifyState();
        resetRequired = true;
        lastMessage = null;
        return (this);
    }

    public void close() throws JMSException {
        verifyState();

        closed = true;

        Reply reply = null;

        try {
            reply = requestRegistry.request(new CloseBrowserRequest(this, dispatchId, browserDispatchId));
        } catch (Exception e) {
            throw ExceptionConverter.convert(e);
        }

        if (!reply.isOk()) {
            throw ExceptionConverter.convert(reply.getException());
        }
        mySession.removeQueueBrowserImpl(this);
    }

    public boolean hasMoreElements() {
        if (closed) {
            return false;
        }

        if (lastMessage == null) {
            FetchBrowserMessageReply reply = null;

            try {
                reply = (FetchBrowserMessageReply) requestRegistry.request(new FetchBrowserMessageRequest(this, dispatchId, browserDispatchId, resetRequired, lastMessageIndex));
                resetRequired = false;
            } catch (Exception e) {
            }

            if (reply.isOk() && reply.getMessageEntry() != null) {
                lastMessage = reply.getMessageEntry().getMessage();
                lastMessageIndex = reply.getMessageEntry().getMessageIndex();
            }
        }

        return (lastMessage != null);
    }

    public Object nextElement() throws NoSuchElementException {
        if (closed || lastMessage == null) {
            throw new NoSuchElementException();
        }

        Message msg = lastMessage;
        lastMessage = null;

        return msg;
    }

}



