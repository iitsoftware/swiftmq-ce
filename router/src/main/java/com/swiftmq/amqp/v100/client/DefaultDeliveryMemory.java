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
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Default implementation of a DeliveryMemory which stores the content in an internal map. It is used when no delivery memory is specified.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 */
public class DefaultDeliveryMemory implements DeliveryMemory {
    String linkName = null;
    LinkedHashMap<DeliveryTag, UnsettledDelivery> unsettled = new LinkedHashMap<DeliveryTag, UnsettledDelivery>();

    public DefaultDeliveryMemory() {
    }

    public synchronized String getLinkName() {
        return linkName;
    }

    public synchronized void setLinkName(String linkName) {
        this.linkName = linkName;
    }

    public synchronized void addUnsettledDelivery(UnsettledDelivery unsettledDelivery) {
        unsettled.put(unsettledDelivery.deliveryTag, unsettledDelivery);
    }

    public synchronized void deliverySettled(DeliveryTag deliveryTag) {
        unsettled.remove(deliveryTag);
    }

    public synchronized int getNumberUnsettled() {
        return unsettled.size();
    }

    public synchronized Collection<UnsettledDelivery> getUnsettled() {
        return ((Map<DeliveryTag, UnsettledDelivery>) unsettled.clone()).values();
    }
}
