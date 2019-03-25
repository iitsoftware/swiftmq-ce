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

package com.swiftmq.impl.routing.single.schedule.po;

import com.swiftmq.impl.routing.single.smqpr.DeliveryRequest;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.POVisitor;

public class PODeliveredObject extends POObject {
    DeliveryRequest deliveryRequest = null;

    public PODeliveredObject(DeliveryRequest deliveryRequest) {
        super(null, null);
        this.deliveryRequest = deliveryRequest;
    }

    public DeliveryRequest getDeliveryRequest() {
        return deliveryRequest;
    }

    public void setDeliveryRequest(DeliveryRequest deliveryRequest) {
        this.deliveryRequest = deliveryRequest;
    }

    public void accept(POVisitor visitor) {
        ((POSchedulerVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[PODeliveredObject, deliveryRequest=" + deliveryRequest + "]";
    }
}
