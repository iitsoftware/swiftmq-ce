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

import com.swiftmq.amqp.v100.generated.transport.definitions.DeliveryTag;

import java.util.Collection;

/**
 * Specifies a memory to store unsettled deliveries of a link (producer and consumer).
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 */
public interface DeliveryMemory {
    /**
     * Will be called from the link to set its link name. This is only done if the name has not been set before and
     * ensures that new created links that use this delivery memory use the same link name as before.
     *
     * @param name
     */
    public void setLinkName(String name);

    /**
     * Returns the link name,
     *
     * @return link name
     */
    public String getLinkName();

    /**
     * Adds an unsettled delivery which consists of a delivery tag, the delivery state and the AMQP message.
     *
     * @param unsettledDelivery unsettled delivery
     */
    public void addUnsettledDelivery(UnsettledDelivery unsettledDelivery);

    /**
     * Removes an unsettled delivery from the memory.
     *
     * @param deliveryTag delivery tag
     */
    public void deliverySettled(DeliveryTag deliveryTag);

    /**
     * Returns the number of unsettled deliveries contained in this memory.
     *
     * @return number unsettled deliveries
     */
    public int getNumberUnsettled();

    /**
     * Returns a collection of all unsettled deliveries. The delivery memory remains untouched so the returned
     * map is a copy (or better a clone) of the content.
     *
     * @return unsettled deliveries
     */
    public Collection<UnsettledDelivery> getUnsettled();
}
