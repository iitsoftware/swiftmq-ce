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
import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.mqtt.v311.netty.buffer.ByteBuf;
import com.swiftmq.mqtt.v311.netty.handler.codec.mqtt.MqttFixedHeader;
import com.swiftmq.mqtt.v311.netty.handler.codec.mqtt.MqttPublishMessage;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.swiftlet.queue.QueueSender;

import javax.jms.DeliveryMode;

public class Producer {
    public static final String PROP_QOS = "JMS_SWIFTMQ_MQTT_PUBQOS";
    SwiftletContext ctx;
    MQTTSession session;
    String topicName;
    TopicImpl topic;
    QueueSender sender;
    QueuePushTransaction transaction = null;

    public Producer(SwiftletContext ctx, MQTTSession session, String topicName) throws Exception {
        this.ctx = ctx;
        this.session = session;
        this.topicName = topicName;
        topic = new TopicImpl(ctx.topicManager.getQueueForTopic(topicName), topicName);
        ctx.topicManager.verifyTopic(topic);
        sender = ctx.queueManager.createQueueSender(topic.getQueueName(), session.getMqttConnection().getActiveLogin());
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", created");
    }

    public void send(MqttPublishMessage mqttPublishMessage) throws Exception {
        MqttFixedHeader fixedHeader = mqttPublishMessage.fixedHeader();
        ByteBuf payload = mqttPublishMessage.payload();
        BytesMessageImpl bytesMessage = new BytesMessageImpl();
        bytesMessage.setJMSDestination(topic);
        bytesMessage.setJMSTimestamp(System.currentTimeMillis());
        bytesMessage.setJMSMessageID(session.getMqttConnection().nextId());
        bytesMessage.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
        bytesMessage.setStringProperty(MessageImpl.PROP_USER_ID, session.getMqttConnection().getActiveLogin().getUserName());
        bytesMessage.setStringProperty(MessageImpl.PROP_CLIENT_ID, session.getClientId());
        if (fixedHeader.isDup())
            bytesMessage.setBooleanProperty(MessageImpl.PROP_DOUBT_DUPLICATE, true);
        bytesMessage.setIntProperty(PROP_QOS, fixedHeader.qosLevel().value());
        payload.reset();
        byte[] b = new byte[payload.size()];
        payload.readBytes(b);
        bytesMessage.writeBytes(b);
        transaction = sender.createTransaction();
        transaction.putMessage(bytesMessage);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", sent: " + bytesMessage);
    }

    public void commit() throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", commit");
        if (transaction != null)
            transaction.commit();
        sender.close();
    }

    @Override
    public String toString() {
        return session.toString() + "/Producer, " +
                "topicName='" + topicName + '\'' +
                ", topic=" + topic +
                '}';
    }
}
