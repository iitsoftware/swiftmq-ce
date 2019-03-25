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

package com.swiftmq.amqp.v100.client;

import com.swiftmq.amqp.v100.generated.messaging.delivery_state.DeliveryStateIF;
import com.swiftmq.amqp.v100.generated.transport.definitions.DeliveryTag;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;

/**
 * Container class to store an unsettled delivery.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 */
public class UnsettledDelivery {
    DeliveryTag deliveryTag = null;
    DeliveryStateIF deliveryStateIF = null;
    AMQPMessage message = null;

    /**
     * Create an UnsettledDelivery.
     *
     * @param deliveryTag     delivery tag
     * @param deliveryStateIF delivery state
     * @param message         AMQP message
     */
    public UnsettledDelivery(DeliveryTag deliveryTag, DeliveryStateIF deliveryStateIF, AMQPMessage message) {
        this.deliveryTag = deliveryTag;
        this.deliveryStateIF = deliveryStateIF;
        this.message = message;
    }

    /**
     * Return the delivery tag
     *
     * @return delivery tag
     */
    public DeliveryTag getDeliveryTag() {
        return deliveryTag;
    }

    /**
     * Returns the AMQP message.
     *
     * @return AMQP message
     */
    public AMQPMessage getMessage() {
        return message;
    }

    /**
     * Returns the delivery state.
     *
     * @return delivery state
     */
    public DeliveryStateIF getDeliveryStateIF() {
        return deliveryStateIF;
    }

    /**
     * Sets the delivery state.
     *
     * @param deliveryStateIF delivery state
     */
    public void setDeliveryStateIF(DeliveryStateIF deliveryStateIF) {
        this.deliveryStateIF = deliveryStateIF;
    }

    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append("[UnsettledDelivery");
        sb.append(", deliveryTag=").append(deliveryTag);
        sb.append(", deliveryStateIF=").append(deliveryStateIF);
        sb.append(", message=").append(message);
        sb.append(']');
        return sb.toString();
    }
}
