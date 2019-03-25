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

import com.swiftmq.jms.ExceptionConverter;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.jms.smqp.v600.*;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestRegistry;
import com.swiftmq.util.SwiftUtilities;

import javax.jms.JMSException;
import javax.jms.ServerSessionPool;
import java.util.ArrayList;
import java.util.List;

public class TopicConnectionConsumerImpl extends ConnectionConsumerImpl implements Recreatable {
    String queueName = null;
    boolean isDurable = false;
    TopicImpl topic = null;
    String messageSelector = null;
    String durableName = null;

    public TopicConnectionConsumerImpl(ConnectionImpl myConnection, int dispatchId, RequestRegistry requestRegistry, ServerSessionPool serverSessionPool, int maxMessages) {
        super(myConnection, dispatchId, requestRegistry, serverSessionPool, maxMessages);
    }

    public Request getRecreateRequest() {
        return null;
    }

    public void setRecreateReply(Reply reply) {
    }

    public List getRecreatables() {
        List list = new ArrayList();
        list.add(new SessionRecreator());
        list.add(new ConsumerRecreator());
        return list;
    }

    void createSubscriber(TopicImpl topic, String messageSelector)
            throws JMSException {
        this.topic = topic;
        this.messageSelector = messageSelector;
        isDurable = false;
        Reply reply = null;

        try {
            reply = requestRegistry.request(new CreateSubscriberRequest(this, dispatchId, topic, messageSelector, false, false));
        } catch (Exception e) {
            throw ExceptionConverter.convert(e);
        }

        if (reply.isOk()) {
            queueName = ((CreateSubscriberReply) reply).getTmpQueueName();
        } else {
            throw ExceptionConverter.convert(reply.getException());
        }
        fillCache();
    }

    void createDurableSubscriber(TopicImpl topic, String messageSelector, String durableName)
            throws JMSException {
        this.topic = topic;
        this.messageSelector = messageSelector;
        this.durableName = durableName;

        isDurable = true;

        try {
            SwiftUtilities.verifyDurableName(durableName);
        } catch (Exception e) {
            throw new JMSException(e.getMessage());
        }

        Reply reply = null;

        try {
            reply = requestRegistry.request(new CreateDurableRequest(this, dispatchId, topic, messageSelector, false, durableName));
        } catch (Exception e) {
            throw ExceptionConverter.convert(e);
        }

        if (reply.isOk()) {
            queueName = ((CreateDurableReply) reply).getQueueName();
        } else {
            throw ExceptionConverter.convert(reply.getException());
        }
        fillCache();
    }

    protected String getQueueName() {
        return queueName;
    }

    private class SessionRecreator implements Recreatable {
        public Request getRecreateRequest() {
            return new CreateSessionRequest(TopicConnectionConsumerImpl.this, 0, false, 0, CreateSessionRequest.TOPIC_SESSION, 0);
        }

        public void setRecreateReply(Reply reply) {
            CreateSessionReply r = (CreateSessionReply) reply;
            dispatchId = r.getSessionDispatchId();
        }

        public List getRecreatables() {
            return null;
        }
    }

    private class ConsumerRecreator implements Recreatable {
        public Request getRecreateRequest() {
            if (isDurable)
                return new CreateDurableRequest(TopicConnectionConsumerImpl.this, dispatchId, topic, messageSelector, false, durableName);
            else
                return new CreateSubscriberRequest(TopicConnectionConsumerImpl.this, dispatchId, topic, messageSelector, false, false);
        }

        public void setRecreateReply(Reply reply) {
        }

        public List getRecreatables() {
            return null;
        }
    }
}
