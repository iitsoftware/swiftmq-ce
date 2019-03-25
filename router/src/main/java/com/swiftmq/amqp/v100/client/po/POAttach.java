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

import com.swiftmq.amqp.v100.client.DeliveryMemory;
import com.swiftmq.amqp.v100.client.Link;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POCallback;
import com.swiftmq.tools.pipeline.POObject;

public abstract class POAttach extends POObject {
    Link link = null;
    DeliveryMemory deliveryMemory;

    protected POAttach(POCallback poCallback, Semaphore semaphore, DeliveryMemory deliveryMemory) {
        super(poCallback, semaphore);
        this.deliveryMemory = deliveryMemory;
    }

    public DeliveryMemory getDeliveryMemory() {
        return deliveryMemory;
    }

    public Link getLink() {
        return link;
    }

    public void setLink(Link link) {
        this.link = link;
    }
}