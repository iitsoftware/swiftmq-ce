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

import javax.jms.*;
import javax.jms.IllegalStateException;

public class QueueConnectionImpl extends ConnectionImpl
        implements QueueConnection {

    protected QueueConnectionImpl(String userName, String password, com.swiftmq.net.client.Connection conn)
            throws JMSException {
        super(userName, password, conn);
    }

    public QueueSession createQueueSession(boolean transacted,
                                           int acknowledgeMode) throws JMSException {
        verifyState();

        SessionImpl queueSession = null;
        com.swiftmq.jms.smqp.v500.CreateSessionReply reply = null;

        try {
            reply =
                    (com.swiftmq.jms.smqp.v500.CreateSessionReply) requestRegistry.request(new com.swiftmq.jms.smqp.v500.CreateSessionRequest(transacted,
                            acknowledgeMode, com.swiftmq.jms.smqp.v500.CreateSessionRequest.QUEUE_SESSION));
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



