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
import com.swiftmq.impl.mqtt.po.POSendMessage;
import com.swiftmq.impl.mqtt.session.MQTTSession;
import com.swiftmq.impl.mqtt.v311.netty.handler.codec.mqtt.MqttQoS;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.swiftlet.queue.MessageProcessor;
import com.swiftmq.swiftlet.queue.QueuePullTransaction;

import java.util.concurrent.atomic.AtomicInteger;

public class Subscription extends MessageProcessor {
    SwiftletContext ctx;
    MQTTSession session;
    String topicName;
    String topicNameTranslated;
    MqttQoS qos;
    boolean closed = false;
    Consumer consumer;
    QueuePullTransaction readTx;
    int subscriptionId;
    Entity usageRegistry = null;
    Entity usageConnection = null;
    final AtomicInteger msgsReceived = new AtomicInteger();
    final AtomicInteger totalMsgsReceived = new AtomicInteger();

    public Subscription(SwiftletContext ctx, MQTTSession session, String topicName, String topicNameTranslated, MqttQoS qos) {
        this.ctx = ctx;
        this.session = session;
        this.topicName = topicName;
        this.topicNameTranslated = topicNameTranslated;
        this.qos = qos;
        this.subscriptionId = session.nextDurableId();
    }

    public Subscription(SwiftletContext ctx, MQTTSession session, SubscriptionStoreEntry subscriptionStoreEntry) {
        this.ctx = ctx;
        this.session = session;
        this.topicName = subscriptionStoreEntry.topicName;
        this.topicNameTranslated = subscriptionStoreEntry.topicNameTranslated;
        this.qos = MqttQoS.valueOf(subscriptionStoreEntry.qos);
        this.subscriptionId = subscriptionStoreEntry.subscriptionId;
    }

    public String getTopicName() {
        return topicName;
    }

    public SubscriptionStoreEntry getStoreEntry() {
        return new SubscriptionStoreEntry(topicName, topicNameTranslated, qos.value(), subscriptionId);
    }

    public void fillRegistryUsage(Entity usageRegistry) {
        this.usageRegistry = usageRegistry;
        try {
            usageRegistry.getProperty("swiftmq-topic").setValue(topicNameTranslated);
            usageRegistry.getProperty("qos").setValue(qos.value());
        } catch (Exception e) {
        }
    }

    public void fillConnectionUsage(Entity usageConnection) {
        this.usageConnection = usageConnection;
        if (usageConnection == null)
            return;
        try {
            usageConnection.getProperty("swiftmq-topic").setValue(topicNameTranslated);
            usageConnection.getProperty("qos").setValue(qos.value());
        } catch (Exception e) {
        }
    }

    public int getMsgsReceived() {
        return msgsReceived.getAndSet(0);
    }

    public int getTotalMsgsReceived() {
        return totalMsgsReceived.get();
    }

    private void incMsgsReceived(int n) {
        if (msgsReceived.get() == Integer.MAX_VALUE)
            msgsReceived.set(0);
        msgsReceived.addAndGet(n);
        if (totalMsgsReceived.get() == Integer.MAX_VALUE)
            totalMsgsReceived.set(0);
        totalMsgsReceived.addAndGet(n);
    }

    public void start() throws Exception {
        if (consumer == null) {
            consumer = session.isPersistent() ? new DurableConsumer(ctx, session, topicNameTranslated, subscriptionId) : new Consumer(ctx, session, topicNameTranslated, subscriptionId);
            readTx = consumer.createReadTransaction();
            consumer.start(this);
        } else
            consumer.restart();
    }

    public void restart() throws Exception {
        if (closed) return;
        consumer.restart();
    }

    public void stop() {
        try {
            consumer.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void processMessage(MessageEntry messageEntry) {
        try {
            QueuePullTransaction tx = consumer.createTransaction();
            tx.moveToTransaction(messageEntry.getMessageIndex(), readTx);
            String jmsTopicName = ((TopicImpl) messageEntry.getMessage().getJMSDestination()).getTopicName();
            MqttQoS mqos = MqttQoS.valueOf(qos.value());
            if (messageEntry.getMessage().propertyExists(Producer.PROP_QOS)) {
                int pqos = messageEntry.getMessage().getIntProperty(Producer.PROP_QOS);
                int sqos = qos.value();
                if (sqos > pqos)
                    mqos = MqttQoS.valueOf(pqos);
            }
            session.getMqttConnection().getConnectionQueue().enqueue(new POSendMessage(jmsTopicName, messageEntry.getMessage(), mqos, tx, this));
            incMsgsReceived(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void processException(Exception e) {
        e.printStackTrace();
    }

    @Override
    public boolean isValid() {
        return !closed;
    }

    public void close() {
        closed = true;
        if (consumer != null)
            consumer.close();
    }

    public void close(ActiveLogin activeLogin) {
        closed = true;
        try {
            ctx.topicManager.deleteDurable(String.valueOf(subscriptionId), activeLogin);
        } catch (Exception e) {
        }

    }
}
