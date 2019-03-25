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
import com.swiftmq.jms.smqp.v630.CreateSessionReply;
import com.swiftmq.jms.smqp.v630.CreateSessionRequest;
import com.swiftmq.net.client.Reconnector;

import javax.jms.JMSException;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;

public class TopicConnectionImpl extends ConnectionImpl
        implements TopicConnection {
    protected TopicConnectionImpl(String userName, String password, Reconnector reconnector)
            throws JMSException {
        super(userName, password, reconnector);
    }

    public TopicSession createTopicSession(boolean transacted, int acknowledgeMode)
            throws JMSException {
        verifyState();

        SessionImpl topicSession = null;
        CreateSessionReply reply = null;

        try {
            reply = (CreateSessionReply) requestRegistry.request(new CreateSessionRequest(0, transacted, acknowledgeMode, CreateSessionRequest.TOPIC_SESSION, 0));
        } catch (Exception e) {
            throw ExceptionConverter.convert(e);
        }

        if (reply.isOk()) {
            int dispatchId = reply.getSessionDispatchId();
            String cid = clientID != null ? clientID : internalCID;
            topicSession = new SessionImpl(SessionImpl.TYPE_TOPIC_SESSION, this, transacted, acknowledgeMode,
                    dispatchId, requestRegistry,
                    myHostname, cid);
            topicSession.setUserName(userName);
            topicSession.setMyDispatchId(addRequestService(topicSession));
            addSession(topicSession);
        } else {
            throw ExceptionConverter.convert(reply.getException());
        }

        return (topicSession);
    }
}

