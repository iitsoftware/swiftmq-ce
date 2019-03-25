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

import com.swiftmq.jms.TopicImpl;
import com.swiftmq.jms.smqp.v610.CreatePublisherReply;
import com.swiftmq.jms.smqp.v610.CreatePublisherRequest;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestRegistry;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import java.util.List;

public class TopicPublisherImpl extends MessageProducerImpl
        implements TopicPublisher, Recreatable {
    Topic topic = null;

    public TopicPublisherImpl(SessionImpl mySession, Topic topic,
                              int producerId, RequestRegistry requestRegistry,
                              String myHostname, String clientId) {
        super(mySession, producerId, requestRegistry, myHostname, clientId);
        this.topic = topic;
    }

    public Request getRecreateRequest() {
        return new CreatePublisherRequest(mySession, mySession.dispatchId, (TopicImpl) topic);
    }

    public void setRecreateReply(Reply reply) {
        producerId = ((CreatePublisherReply) reply).getTopicPublisherId();
    }

    public List getRecreatables() {
        return null;
    }

    public Topic getTopic()
            throws JMSException {
        verifyState();

        return (topic);
    }

    public void publish(Message message)
            throws JMSException {
        send(message);
    }

    public void publish(Message message, int deliveryMode, int priority, long timeToLive)
            throws JMSException {
        send(message, deliveryMode, priority, timeToLive);
    }

    public void publish(Topic topic, Message message)
            throws JMSException {
        send(topic, message);
    }

    public void publish(Topic topic, Message message, int deliveryMode, int priority, long timeToLive)
            throws JMSException {
        send(topic, message, deliveryMode, priority, timeToLive);
    }
}

