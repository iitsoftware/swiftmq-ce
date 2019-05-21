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

package com.swiftmq.impl.jmsbridge;

import com.swiftmq.jms.MessageCloner;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import javax.jms.*;

public class RemoteQueueBridgeSink implements BridgeSink {
    SwiftletContext ctx = null;
    Queue queue = null;
    QueueSession session = null;
    QueueSender sender = null;
    String mode = null;
    int messagesTransfered = 0;
    long size = 0;
    Collector collector = null;
    DataByteArrayOutputStream dbos = null;

    RemoteQueueBridgeSink(SwiftletContext ctx, QueueConnection connection, Queue queue) throws Exception {
        this.ctx = ctx;
        this.queue = queue;
        session = connection.createQueueSession(true, Session.CLIENT_ACKNOWLEDGE);
        sender = session.createSender(queue);
    }

    public void setCollector(Collector collector) {
        this.collector = collector;
    }

    public void setPersistenceMode(String mode) throws Exception {
        this.mode = mode;
        if (mode.equals("persistent"))
            sender.setDeliveryMode(DeliveryMode.PERSISTENT);
        else if (mode.equals("nonpersistent"))
            sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
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
            sender.send(MessageCloner.cloneMessage(msg, session));
        else
            sender.send(MessageCloner.cloneMessage(msg, session),
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
            sender.close();
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
            name = queue.getQueueName();
        } catch (Exception ignored) {
        }
        return "[RemoteQueueBridgeSink, queue=" + name + "]";
    }
}

