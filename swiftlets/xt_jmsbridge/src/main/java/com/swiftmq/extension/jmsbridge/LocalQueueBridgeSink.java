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
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.util.IdGenerator;

import javax.jms.DeliveryMode;
import javax.jms.Message;

public class LocalQueueBridgeSink implements BridgeSink {
    SwiftletContext ctx = null;
    String queueName = null;
    QueueImpl queue = null;
    com.swiftmq.swiftlet.queue.QueueSender sender = null;
    QueuePushTransaction pushTransaction = null;
    String msgIdPrefix = null;
    long msgInc = 0;
    int messagesTransfered = 0;
    long size = 0;
    String mode = null;
    Collector collector = null;
    DataByteArrayOutputStream dbos = null;

    LocalQueueBridgeSink(SwiftletContext ctx, String queueName) throws Exception {
        this.ctx = ctx;
        this.queueName = queueName;
        queue = new QueueImpl(queueName);
        sender = ctx.queueManager.createQueueSender(queueName, null);
        msgIdPrefix = IdGenerator.getInstance().nextId('/') + "/" + SwiftletManager.getInstance().getRouterName() + "/jmsbridge/" + System.currentTimeMillis() + "/";
    }

    public void setCollector(Collector collector) {
        this.collector = collector;
    }

    public void setPersistenceMode(String mode) throws Exception {
        this.mode = mode;
    }

    public void putMessage(Message message)
            throws Exception {
        if (pushTransaction != null)
            throw new Exception("previous push transaction was not orderly closed");
        MessageImpl msg = (MessageImpl) MessageCloner.cloneMessage(message);
        msg.setJMSDestination(queue);
        msg.setJMSTimestamp(System.currentTimeMillis());
        if (mode.equals("persistent"))
            msg.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
        else if (mode.equals("nonpersistent"))
            msg.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
        else
            msg.setJMSDeliveryMode(message.getJMSDeliveryMode());
        msg.setJMSPriority(message.getJMSPriority());
        msg.setJMSMessageID(msgIdPrefix + (msgInc++));
        if (collector != null && collector.requiresSize() && msg.getMessageLength() <= 0) {
            if (dbos == null)
                dbos = new DataByteArrayOutputStream();
            msg.writeContent(dbos);
            size += dbos.getCount();
            dbos.rewind();
        } else
            size += msg.getMessageLength();

        pushTransaction = sender.createTransaction();
        pushTransaction.putMessage((MessageImpl) msg);
        messagesTransfered++;
    }

    public void commit()
            throws Exception {
        pushTransaction.commit();
        pushTransaction = null;
        if (collector != null)
            collector.collect(messagesTransfered, size);
        messagesTransfered = 0;
        size = 0;
    }

    public void rollback()
            throws Exception {
        pushTransaction.rollback();
        pushTransaction = null;
        messagesTransfered = 0;
        size = 0;
    }

    public void destroy() {
        if (pushTransaction != null) {
            try {
                pushTransaction.rollback();
                pushTransaction = null;
            } catch (Exception ignored) {
            }
        }
        try {
            sender.close();
        } catch (Exception ignored) {
        }
    }

    public String toString() {
        return "[LocalQueueBridgeSink, queue=" + queueName + "]";
    }

}

