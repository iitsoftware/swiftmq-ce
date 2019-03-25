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

package com.swiftmq.mqtt.v311.netty.handler.codec.mqtt;

import com.swiftmq.mqtt.v311.netty.buffer.ByteBuf;
import com.swiftmq.mqtt.v311.netty.handler.codec.DecoderResult;

/**
 * Utility class with factory methods to create different types of MQTT messages.
 */
public final class MqttMessageFactory {

    public static MqttMessage newMessage(MqttFixedHeader mqttFixedHeader, Object variableHeader, Object payload) {
        switch (mqttFixedHeader.messageType()) {
            case CONNECT:
                return new MqttConnectMessage(
                        mqttFixedHeader,
                        (MqttConnectVariableHeader) variableHeader,
                        (MqttConnectPayload) payload);

            case CONNACK:
                return new MqttConnAckMessage(mqttFixedHeader, (MqttConnAckVariableHeader) variableHeader);

            case SUBSCRIBE:
                return new MqttSubscribeMessage(
                        mqttFixedHeader,
                        (MqttMessageIdVariableHeader) variableHeader,
                        (MqttSubscribePayload) payload);

            case SUBACK:
                return new MqttSubAckMessage(
                        mqttFixedHeader,
                        (MqttMessageIdVariableHeader) variableHeader,
                        (MqttSubAckPayload) payload);

            case UNSUBACK:
                return new MqttUnsubAckMessage(
                        mqttFixedHeader,
                        (MqttMessageIdVariableHeader) variableHeader);

            case UNSUBSCRIBE:
                return new MqttUnsubscribeMessage(
                        mqttFixedHeader,
                        (MqttMessageIdVariableHeader) variableHeader,
                        (MqttUnsubscribePayload) payload);

            case PUBLISH:
                return new MqttPublishMessage(
                        mqttFixedHeader,
                        (MqttPublishVariableHeader) variableHeader,
                        (ByteBuf) payload);

            case PUBACK:
                return new MqttPubAckMessage(mqttFixedHeader, (MqttMessageIdVariableHeader) variableHeader);
            case PUBREC:
            case PUBREL:
            case PUBCOMP:
                return new MqttMessage(mqttFixedHeader, variableHeader);

            case PINGREQ:
            case PINGRESP:
            case DISCONNECT:
                return new MqttMessage(mqttFixedHeader);

            default:
                throw new IllegalArgumentException("unknown message type: " + mqttFixedHeader.messageType());
        }
    }

    public static MqttMessage newInvalidMessage(Throwable cause) {
        cause.printStackTrace();
        return new MqttMessage(null, null, null, DecoderResult.failure(cause));
    }

    private MqttMessageFactory() {
    }
}
