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

package com.swiftmq.jms.v610;

import com.swiftmq.jms.ExceptionConverter;
import com.swiftmq.jms.smqp.v610.CreateSessionReply;
import com.swiftmq.jms.smqp.v610.CreateSessionRequest;
import com.swiftmq.net.client.Reconnector;

import javax.jms.*;
import javax.jms.IllegalStateException;

public class QueueConnectionImpl extends ConnectionImpl
        implements QueueConnection {

    protected QueueConnectionImpl(String userName, String password, Reconnector reconnector)
            throws JMSException {
        super(userName, password, reconnector);
    }

    public QueueSession createQueueSession(boolean transacted,
                                           int acknowledgeMode) throws JMSException {
        verifyState();

        SessionImpl queueSession = null;
        CreateSessionReply reply = null;

        try {
            reply = (CreateSessionReply) requestRegistry.request(new CreateSessionRequest(0, transacted, acknowledgeMode, CreateSessionRequest.QUEUE_SESSION, 0));
        } catch (Exception e) {
            throw ExceptionConverter.convert(e);
        }

        if (reply.isOk()) {
            int dispatchId = reply.getSessionDispatchId();

            queueSession = new SessionImpl(SessionImpl.TYPE_QUEUE_SESSION, this, transacted, acknowledgeMode,
                    dispatchId, requestRegistry,
                    myHostname, null);
            queueSession.setUserName(getUserName());
            queueSession.setMyDispatchId(addRequestService(queueSession));
            addSession(queueSession);
        } else {
            throw ExceptionConverter.convert(reply.getException());
        }

        return (queueSession);
    }

    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages)
            throws JMSException {
        throw new IllegalStateException("Operation not allowed on this connection type");
    }
}



