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

package com.swiftmq.amqp.v100.client.po;

import com.swiftmq.amqp.v100.client.Producer;
import com.swiftmq.amqp.v100.client.QoS;
import com.swiftmq.amqp.v100.client.SessionVisitor;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.DeliveryStateIF;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TxnIdIF;
import com.swiftmq.amqp.v100.generated.transport.definitions.DeliveryTag;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.transport.Packager;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.POVisitor;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.IOException;

public class POSendMessage extends POObject {
    Producer producer;
    AMQPMessage message;
    TxnIdIF txnId;
    DeliveryStateIF deliveryState = null;
    DeliveryTag deliveryTag = null;
    Packager packager = new Packager();
    boolean recovery = false;

    public POSendMessage(Semaphore semaphore, Producer producer, AMQPMessage message, TxnIdIF txnId, DeliveryTag deliveryTag) throws IOException {
        super(null, semaphore);
        this.producer = producer;
        this.message = message;
        this.txnId = txnId;
        this.deliveryTag = deliveryTag;
        DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();
        message.writeContent(dbos);
        packager.setChannel(producer.getMySession().getChannel());
        packager.setHandle(producer.getHandle());
        packager.setSettled(producer.getQoS() == QoS.AT_MOST_ONCE);
        packager.setData(dbos.getBuffer(), dbos.getCount());
        dbos = null;
    }

    public Producer getProducer() {
        return producer;
    }

    public synchronized DeliveryStateIF getDeliveryState() {
        return deliveryState;
    }

    public synchronized void setDeliveryState(DeliveryStateIF deliveryState) {
        this.deliveryState = deliveryState;
    }

    public DeliveryTag getDeliveryTag() {
        return deliveryTag;
    }

    public TxnIdIF getTxnId() {
        return txnId;
    }

    public AMQPMessage getMessage() {
        return message;
    }

    public Packager getPackager() {
        return packager;
    }

    public boolean isRecovery() {
        return recovery;
    }

    public void setRecovery(boolean recovery) {
        this.recovery = recovery;
    }

    public void accept(POVisitor visitor) {
        ((SessionVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[POSendMessage, producer=" + producer + ", message=" + message + ", txnId=" + txnId + ", deliveryState=" + deliveryState + ", deliveryTag=" + deliveryTag + ", recovery=" + recovery + "]";
    }
}