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
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.smqp.v750.CreateConsumerRequest;
import com.swiftmq.jms.smqp.v750.CreateSessionReply;
import com.swiftmq.jms.smqp.v750.CreateSessionRequest;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestRegistry;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.ServerSessionPool;
import java.util.ArrayList;
import java.util.List;

public class QueueConnectionConsumerImpl extends ConnectionConsumerImpl implements Recreatable {
    String queueName = null;
    Queue queue = null;
    String messageSelector = null;

    public QueueConnectionConsumerImpl(ConnectionImpl myConnection, int dispatchId, RequestRegistry requestRegistry, ServerSessionPool serverSessionPool, int maxMessages) {
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

    void createConsumer(QueueImpl queue, String messageSelector) throws JMSException {
        if (queue == null)
            throw new NullPointerException("createConsumer, queue is null!");

        queueName = queue.getQueueName();
        this.queue = queue;
        this.messageSelector = messageSelector;

        Reply reply = null;
        try {
            reply = requestRegistry.request(new CreateConsumerRequest(this, dispatchId, (QueueImpl) queue, messageSelector));
        } catch (Exception e) {
            throw ExceptionConverter.convert(e);
        }
        if (!reply.isOk())
            throw ExceptionConverter.convert(reply.getException());
        fillCache();
    }

    protected String getQueueName() {
        return queueName;
    }

    private class SessionRecreator implements Recreatable {
        public Request getRecreateRequest() {
            return new CreateSessionRequest(QueueConnectionConsumerImpl.this, 0, false, 0, CreateSessionRequest.QUEUE_SESSION, 0);
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
            return new CreateConsumerRequest(QueueConnectionConsumerImpl.this, dispatchId, (QueueImpl) queue, messageSelector);
        }

        public void setRecreateReply(Reply reply) {
        }

        public List getRecreatables() {
            return null;
        }
    }
}
