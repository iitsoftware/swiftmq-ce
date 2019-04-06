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

package com.swiftmq.impl.mqtt.v311.netty.handler.codec.mqtt;

import com.swiftmq.impl.mqtt.v311.netty.buffer.ByteBuf;

/**
 * See <a href="http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#publish">MQTTV3.1/publish</a>
 */
public class MqttPublishMessage extends MqttMessage {

    public MqttPublishMessage(
            MqttFixedHeader mqttFixedHeader,
            MqttPublishVariableHeader variableHeader,
            com.swiftmq.impl.mqtt.v311.netty.buffer.ByteBuf payload) {
        super(mqttFixedHeader, variableHeader, payload);
    }

    @Override
    public MqttPublishVariableHeader variableHeader() {
        return (MqttPublishVariableHeader) super.variableHeader();
    }

    @Override
    public com.swiftmq.impl.mqtt.v311.netty.buffer.ByteBuf payload() {
        return content();
    }

    public com.swiftmq.impl.mqtt.v311.netty.buffer.ByteBuf content() {
        final com.swiftmq.impl.mqtt.v311.netty.buffer.ByteBuf data = (com.swiftmq.impl.mqtt.v311.netty.buffer.ByteBuf) super.payload();
        return data;
    }

    public MqttPublishMessage copy() {
        return replace(content().duplicate());
    }

    public MqttPublishMessage replace(ByteBuf content) {
        return new MqttPublishMessage(fixedHeader(), variableHeader(), content);
    }

}
