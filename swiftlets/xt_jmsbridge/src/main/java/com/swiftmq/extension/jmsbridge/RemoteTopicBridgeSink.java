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

package com.swiftmq.extension.jmsbridge;

import com.swiftmq.jms.MessageCloner;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import javax.jms.*;

public class RemoteTopicBridgeSink implements BridgeSink {
    SwiftletContext ctx = null;
    Topic topic = null;
    TopicSession session = null;
    TopicPublisher publisher = null;
    String mode = null;
    int messagesTransfered = 0;
    long size = 0;
    Collector collector = null;
    DataByteArrayOutputStream dbos = null;

    RemoteTopicBridgeSink(SwiftletContext ctx, TopicConnection connection, Topic topic) throws Exception {
        this.ctx = ctx;
        this.topic = topic;
        session = connection.createTopicSession(true, Session.CLIENT_ACKNOWLEDGE);
        publisher = session.createPublisher(topic);
    }

    public void setCollector(Collector collector) {
        this.collector = collector;
    }

    public void setPersistenceMode(String mode) throws Exception {
        this.mode = mode;
        if (mode.equals("persistent"))
            publisher.setDeliveryMode(DeliveryMode.PERSISTENT);
        else if (mode.equals("nonpersistent"))
            publisher.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
    }

    Session getSession() {
        return session;
    }

    public void putMessage(Message msg)
            throws Exception {
        long n = ((MessageImpl) msg).getMessageLength();
        if (collector != null && collector.requiresSize() && n <= 0) {
            if (dbos == null)
                dbos = new DataByteArrayOutputStream();
            ((MessageImpl) msg).writeContent(dbos);
            size += dbos.getCount();
            dbos.rewind();
        } else
            size += n;

        if (!mode.equals("as_source"))
            publisher.publish(MessageCloner.cloneMessage(msg, session));
        else
            publisher.publish(MessageCloner.cloneMessage(msg, session),
                    msg.getJMSDeliveryMode(),
                    msg.getJMSPriority(),
                    msg.getJMSExpiration());
        messagesTransfered++;
    }

    public void commit()
            throws Exception {
        session.commit();
        if (collector != null)
            collector.collect(messagesTransfered, size);
        messagesTransfered = 0;
        size = 0;
    }

    public void rollback()
            throws Exception {
        session.rollback();
        messagesTransfered = 0;
        size = 0;
    }

    public void destroy() {
        try {
            publisher.close();
        } catch (Exception ignored) {
        }
        try {
            session.close();
        } catch (Exception ignored) {
        }
    }

    public String toString() {
        String name = null;
        try {
            name = topic.getTopicName();
        } catch (Exception ignored) {
        }
        return "[RemoteTopicBridgeSink, topic=" + name + "]";
    }
}

