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

package com.swiftmq.impl.scheduler.standard.job;

import com.swiftmq.impl.scheduler.standard.SwiftletContext;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.swiftlet.scheduler.Job;
import com.swiftmq.swiftlet.scheduler.JobException;
import com.swiftmq.swiftlet.scheduler.JobTerminationListener;
import com.swiftmq.tools.concurrent.AtomicWrappingCounterLong;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.util.IdGenerator;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

public class MessageSenderJob implements Job {
    private static final String PREFIX = "sys$scheduler-" + IdGenerator.getInstance().nextId('-') + '-';
    private static final AtomicWrappingCounterLong msgNo = new AtomicWrappingCounterLong(1);
    SwiftletContext ctx = null;
    String id = null;

    public MessageSenderJob(SwiftletContext ctx) {
        this.ctx = ctx;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/created");
    }

    private static String nextJMSMessageId() {
        return PREFIX + msgNo.getAndIncrement();
    }

    private MessageImpl copyMessage(MessageImpl msg) throws Exception {
        DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();
        DataByteArrayInputStream dbis = new DataByteArrayInputStream();
        dbos.rewind();
        msg.writeContent(dbos);
        dbis.reset();
        dbis.setBuffer(dbos.getBuffer(), 0, dbos.getCount());
        MessageImpl msgCopy = MessageImpl.createInstance(dbis.readInt());
        msgCopy.readContent(dbis);
        return msgCopy;
    }

    private MessageImpl getMessage() throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/getMessage ...");
        MessageSelector selector = new MessageSelector(SwiftletContext.PROP_SCHED_ID + " = '" + id + "'");
        selector.compile();
        QueueReceiver receiver = ctx.queueManager.createQueueReceiver(SwiftletContext.INTERNAL_QUEUE, null, selector);
        QueuePullTransaction t = receiver.createTransaction(false);
        MessageEntry entry = t.getMessage(0, selector);
        t.rollback();
        receiver.close();
        if (entry == null)
            throw new Exception("Schedule with ID '" + id + "' not found!");
        MessageImpl msg = copyMessage(entry.getMessage());
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/getMessage done");
        return msg;

    }

    private void sendToQueue(String queueName, MessageImpl msg) throws Exception {
        if (!ctx.queueManager.isQueueDefined(queueName))
            throw new Exception("Queue '" + queueName + "' is undefined!");
        QueueSender sender = ctx.queueManager.createQueueSender(queueName, null);
        QueuePushTransaction t = sender.createTransaction();
        msg.setJMSDestination(new QueueImpl(queueName));
        msg.setJMSTimestamp(System.currentTimeMillis());
        msg.setJMSMessageID(nextJMSMessageId());
        t.putMessage(msg);
        t.commit();
        sender.close();
    }

    private void sendToTopic(String topicName, MessageImpl msg) throws Exception {
        if (!ctx.topicManager.isTopicDefined(topicName))
            throw new Exception("Topic '" + topicName + "' is undefined!");
        String queueName = ctx.topicManager.getQueueForTopic(topicName);
        QueueSender sender = ctx.queueManager.createQueueSender(queueName, null);
        QueuePushTransaction t = sender.createTransaction();
        msg.setJMSDestination(new TopicImpl(topicName));
        msg.setJMSTimestamp(System.currentTimeMillis());
        msg.setJMSMessageID(nextJMSMessageId());
        t.putMessage(msg);
        t.commit();
        sender.close();
    }

    public void start(Properties properties, JobTerminationListener jobTerminationListener) throws JobException {
        id = properties.getProperty(SwiftletContext.PARM);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/start ...");
        try {
            MessageImpl msg = getMessage();
            String type = msg.getStringProperty(SwiftletContext.PROP_SCHED_DEST_TYPE).toLowerCase();
            String dest = msg.getStringProperty(SwiftletContext.PROP_SCHED_DEST);
            long expiration = 0;
            if (msg.propertyExists(SwiftletContext.PROP_SCHED_EXPIRATION))
                expiration = msg.getLongProperty(SwiftletContext.PROP_SCHED_EXPIRATION);
            List<String> list = new ArrayList<>();
            for (Enumeration _enum = msg.getPropertyNames(); _enum.hasMoreElements(); ) {
                String name = (String) _enum.nextElement();
                if (name.startsWith("JMS_SWIFTMQ_SCHEDULER"))
                    list.add(name);
            }
            for (String s : list) msg.removeProperty((String) s);
            if (expiration > 0)
                msg.setJMSExpiration(System.currentTimeMillis() + expiration);
            msg.setJMSReplyTo(null);
            if (type.equals(SwiftletContext.QUEUE))
                sendToQueue(dest, msg);
            else
                sendToTopic(dest, msg);
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/start, exception=" + e);
            throw new JobException(e.getMessage(), e, false);
        }
        jobTerminationListener.jobTerminated();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/start done");
    }

    public void stop() throws JobException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/stop, do nothing");
    }

    public String toString() {
        return "[MessageSenderJob, id=" + id + "]";
    }
}
