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

import com.swiftmq.impl.mqtt.v311.netty.handler.codec.mqtt.MqttConnectMessage;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.POVisitor;

public class POConnect extends POObject {
    MqttConnectMessage message;

    public POConnect(MqttConnectMessage message) {
        super(null, null);
        this.message = message;
    }

    public MqttConnectMessage getMessage() {
        return message;
    }

    public void accept(POVisitor visitor) {
        ((MQTTVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[POConnect, message=" + message + "]";
    }
}