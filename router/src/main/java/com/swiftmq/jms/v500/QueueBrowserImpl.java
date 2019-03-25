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

/*--- formatted by Jindent 2.1, (www.c-lab.de/~jindent) ---*/

package com.swiftmq.jms.v500;

import com.swiftmq.jms.ExceptionConverter;
import com.swiftmq.jms.smqp.v500.FetchBrowserMessageReply;
import com.swiftmq.jms.smqp.v500.FetchBrowserMessageRequest;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.RequestRegistry;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import java.util.Enumeration;
import java.util.NoSuchElementException;

/**
 * @author Andreas Mueller, IIT GmbH
 * @version 1.0
 */
public class QueueBrowserImpl implements QueueBrowser, Enumeration {
    boolean closed = false;
    Queue queue = null;
    String messageSelector = null;
    int dispatchId = -1;
    int browserDispatchId = -1;
    RequestRegistry requestRegistry = null;
    Message lastMessage = null;
    SessionImpl mySession = null;
    boolean resetRequired = false;

    /**
     * Constructor declaration
     *
     * @param mySession
     * @param queue
     * @param messageSelector
     * @param dispatchId
     * @param browserDispatchId
     * @param requestRegistry
     * @see
     */
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

    /**
     * Method declaration
     *
     * @throws JMSException
     * @see
     */
    protected void verifyState() throws JMSException {
        if (closed) {
            throw new JMSException("Queue browser is closed");
        }

        mySession.verifyState();
    }

    /**
     * Get the queue associated with this queue browser.
     *
     * @return the queue
     * @throws JMSException if JMS fails to get the
     *                      queue associated with this Browser
     *                      due to some JMS error.
     */
    public Queue getQueue() throws JMSException {
        verifyState();

        return (queue);
    }

    /**
     * Get this queue browser's message selector expression.
     *
     * @return this queue browser's message selector
     * @throws JMSException if JMS fails to get the
     *                      message selector for this browser
     *                      due to some JMS error.
     */
    public String getMessageSelector() throws JMSException {
        verifyState();

        return messageSelector;
    }

    /**
     * Get an enumeration for browsing the current queue messages in the
     * order they would be received.
     *
     * @return an enumeration for browsing the messages
     * @throws JMSException if JMS fails to get the
     *                      enumeration for this browser
     *                      due to some JMS error.
     */
    public Enumeration getEnumeration() throws JMSException {
        verifyState();
        resetRequired = true;
        lastMessage = null;
        return (this);
    }

    /**
     * Since a provider may allocate some resources on behalf of a
     * QueueBrowser outside the JVM, clients should close them when they
     * are not needed. Relying on garbage collection to eventually reclaim
     * these resources may not be timely enough.
     *
     * @throws JMSException if a JMS fails to close this
     *                      Browser due to some JMS error.
     */
    public void close() throws JMSException {
        verifyState();

        closed = true;

        Reply reply = null;

        try {
            reply = requestRegistry.request(new com.swiftmq.jms.smqp.v500.CloseBrowserRequest(dispatchId,
                    browserDispatchId));
        } catch (Exception e) {
            throw ExceptionConverter.convert(e);
        }

        if (!reply.isOk()) {
            throw ExceptionConverter.convert(reply.getException());
        }
    }

    /**
     * Method declaration
     *
     * @return
     * @see
     */
    public boolean hasMoreElements() {
        if (closed) {
            return false;
        }

        if (lastMessage == null) {
            FetchBrowserMessageReply reply = null;

            try {
                reply = (FetchBrowserMessageReply) requestRegistry.request(new FetchBrowserMessageRequest(dispatchId, browserDispatchId, resetRequired));
                resetRequired = false;
            } catch (Exception e) {
            }

            if (reply.isOk() && reply.getMessageEntry() != null) {
                lastMessage = reply.getMessageEntry().getMessage();
            }
        }

        return (lastMessage != null);
    }

    /**
     * Method declaration
     *
     * @return
     * @throws NoSuchElementException
     * @see
     */
    public Object nextElement() throws NoSuchElementException {
        if (closed || lastMessage == null) {
            throw new NoSuchElementException();
        }

        Message msg = lastMessage;

        lastMessage = null;

        return msg;
    }

}



