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

package com.swiftmq.jms.v510;

import com.swiftmq.jms.ExceptionConverter;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.jms.smqp.v510.CreateDurableReply;
import com.swiftmq.jms.smqp.v510.CreateDurableRequest;
import com.swiftmq.jms.smqp.v510.CreateSubscriberReply;
import com.swiftmq.jms.smqp.v510.CreateSubscriberRequest;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.RequestRegistry;
import com.swiftmq.util.SwiftUtilities;

import javax.jms.JMSException;
import javax.jms.ServerSessionPool;

public class TopicConnectionConsumerImpl extends ConnectionConsumerImpl {
    String queueName = null;

    public TopicConnectionConsumerImpl(ConnectionImpl myConnection, int dispatchId, RequestRegistry requestRegistry, ServerSessionPool serverSessionPool, int maxMessages) {
        super(myConnection, dispatchId, requestRegistry, serverSessionPool, maxMessages);
    }

    void createSubscriber(TopicImpl topic, String messageSelector)
            throws JMSException {
        Reply reply = null;

        try {
            reply = requestRegistry.request(new CreateSubscriberRequest(dispatchId, topic, messageSelector, false, false));
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

        try {
            SwiftUtilities.verifyDurableName(durableName);
        } catch (Exception e) {
            throw new JMSException(e.getMessage());
        }

        Reply reply = null;

        try {
            reply = requestRegistry.request(new CreateDurableRequest(dispatchId, topic, messageSelector, false, durableName));
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
}
