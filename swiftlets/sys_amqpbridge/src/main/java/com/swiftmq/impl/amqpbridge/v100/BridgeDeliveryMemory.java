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

package com.swiftmq.impl.amqpbridge.v100;

import com.swiftmq.amqp.v100.client.DefaultDeliveryMemory;
import com.swiftmq.amqp.v100.client.InvalidStateException;
import com.swiftmq.amqp.v100.generated.transport.definitions.DeliveryTag;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.impl.amqpbridge.SwiftletContext;

import java.util.LinkedHashMap;

public class BridgeDeliveryMemory extends DefaultDeliveryMemory {
    SwiftletContext ctx;
    String tracePrefix;
    LinkedHashMap<DeliveryTag, AMQPMessage> unsettledSourceMessages = new LinkedHashMap<DeliveryTag, AMQPMessage>();

    public BridgeDeliveryMemory(SwiftletContext ctx, String tracePrefix) {
        this.ctx = ctx;
        this.tracePrefix = tracePrefix;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), tracePrefix + "/" + toString() + "/created");
    }

    public void addUnsettledSourceMessage(DeliveryTag deliveryTag, AMQPMessage message) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), tracePrefix + "/" + toString() + "/addUnsettledSourceMessage, deliveryTag=" + deliveryTag.getValueString());
        unsettledSourceMessages.put(deliveryTag, message);
    }

    public void deliverySettled(DeliveryTag deliveryTag) {
        AMQPMessage sourceMessage = unsettledSourceMessages.remove(deliveryTag);
        if (sourceMessage != null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), tracePrefix + "/" + toString() + "/deliverySettled, deliveryTag=" + deliveryTag.getValueString());
            try {
                sourceMessage.accept();
            } catch (InvalidStateException e) {
            }
        } else if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), tracePrefix + "/" + toString() + "/deliverySettled, deliveryTag=" + deliveryTag.getValueString() + " NOT FOUND!");

        super.deliverySettled(deliveryTag);
    }

    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append("BridgeDeliveryMemory");
        sb.append(" unsettledSourceMessages.size=" + unsettledSourceMessages.size());
        return sb.toString();
    }
}
