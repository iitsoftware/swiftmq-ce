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

package com.swiftmq.impl.mqtt.po;

import com.swiftmq.impl.mqtt.pubsub.Subscription;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.mqtt.v311.netty.handler.codec.mqtt.MqttQoS;
import com.swiftmq.swiftlet.queue.QueueTransaction;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.POVisitor;

public class POSendMessage extends POObject {
    String topicName;
    MessageImpl jmsMessage;
    MqttQoS qos;
    QueueTransaction transaction;
    Subscription subscription;

    public POSendMessage(String topicName, MessageImpl jmsMessage, MqttQoS qos, QueueTransaction transaction, Subscription subscription) {
        super(null, null);
        this.topicName = topicName;
        this.jmsMessage = jmsMessage;
        this.qos = qos;
        this.transaction = transaction;
        this.subscription = subscription;
    }

    public String getTopicName() {
        return topicName;
    }

    public MessageImpl getJmsMessage() {
        return jmsMessage;
    }

    public MqttQoS getQos() {
        return qos;
    }

    public QueueTransaction getTransaction() {
        return transaction;
    }

    public Subscription getSubscription() {
        return subscription;
    }

    public void accept(POVisitor visitor) {
        ((MQTTVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[POSendMessage, topicName=" + topicName + ", qos=" + qos.toString() + "]";
    }
}