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

package com.swiftmq.impl.mqtt.pubsub;

import com.swiftmq.impl.mqtt.SwiftletContext;
import com.swiftmq.impl.mqtt.session.MQTTSession;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.swiftlet.queue.MessageProcessor;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.queue.QueuePullTransaction;
import com.swiftmq.swiftlet.queue.QueueReceiver;

public class Consumer {
    SwiftletContext ctx;
    MQTTSession session;
    String topicName;
    String queueName;
    int subscriberId;
    TopicImpl topic;
    protected QueueReceiver queueReceiver;
    protected QueuePullTransaction readTransaction = null;
    protected QueuePullTransaction transaction = null;
    MessageProcessor messageProcessor = null;
    int subscriptionId;


    public Consumer(SwiftletContext ctx, MQTTSession session, String topicName, int subscriptionId) throws Exception {
        this.ctx = ctx;
        this.session = session;
        this.topicName = topicName;
        this.subscriptionId = subscriptionId;
        topic = ctx.topicManager.verifyTopic(new TopicImpl(topicName));
        queueReceiver = createReceiver(subscriptionId);
    }

    protected QueueReceiver createReceiver(int subscriptionId) throws Exception {
        queueName = ctx.queueManager.createTemporaryQueue();
        subscriberId = ctx.topicManager.subscribe(topic, null, false, queueName, session.getMqttConnection().getActiveLogin(), true);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + "/createReceiver, queueName=" + queueName + ", subsciberId=" + subscriberId);
        return ctx.queueManager.createQueueReceiver(queueName, session.getMqttConnection().getActiveLogin(), null);
    }

    public QueuePullTransaction createTransaction() throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + "/createTransaction");
        transaction = queueReceiver.createTransaction(true);
        return transaction;
    }

    public QueuePullTransaction createReadTransaction() throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + "/createReadTransaction");
        readTransaction = queueReceiver.createTransaction(false);
        return readTransaction;
    }

    public void start(MessageProcessor messageProcessor) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + "/start");
        this.messageProcessor = messageProcessor;
        restart();
    }

    public void restart() throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + "/restart");
        readTransaction.registerMessageProcessor(messageProcessor);
    }

    public void stop() throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + "/stop");
        if (readTransaction != null && !readTransaction.isClosed())
            if (messageProcessor != null)
                readTransaction.unregisterMessageProcessor(messageProcessor);
        if (transaction != null && !transaction.isClosed())
            transaction.rollback();
    }

    public void close() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + "/close");
        try {
            if (transaction != null && !transaction.isClosed())
                transaction.rollback();
            if (readTransaction != null && !readTransaction.isClosed())
                readTransaction.rollback();
            if (queueReceiver != null)
                queueReceiver.close();
            ctx.topicManager.unsubscribe(subscriberId);
            ctx.queueManager.deleteTemporaryQueue(queueName);
        } catch (QueueException e) {
        }
    }

    @Override
    public String toString() {
        return session.toString() + "/Consumer, " +
                "topicName='" + topicName + "'";
    }
}
