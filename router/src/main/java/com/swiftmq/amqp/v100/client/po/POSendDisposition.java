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

import com.swiftmq.amqp.v100.client.Consumer;
import com.swiftmq.amqp.v100.client.SessionVisitor;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.DeliveryStateIF;
import com.swiftmq.amqp.v100.generated.transport.definitions.DeliveryTag;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.POVisitor;

public class POSendDisposition extends POObject {
    Consumer consumer = null;
    long deliveryId = 0;
    DeliveryTag deliveryTag = null;
    DeliveryStateIF deliveryState;

    public POSendDisposition(Consumer consumer, long deliveryId, DeliveryTag deliveryTag, DeliveryStateIF deliveryState) {
        super(null, null);
        this.consumer = consumer;
        this.deliveryId = deliveryId;
        this.deliveryTag = deliveryTag;
        this.deliveryState = deliveryState;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public long getDeliveryId() {
        return deliveryId;
    }

    public DeliveryTag getDeliveryTag() {
        return deliveryTag;
    }

    public DeliveryStateIF getDeliveryState() {
        return deliveryState;
    }

    public void accept(POVisitor visitor) {
        ((SessionVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[POSendDisposition, consumer=" + consumer + ", deliveryId=" + deliveryId + ", deliveryTag=" + deliveryTag + ", deliveryState=" + deliveryState + "]";
    }
}