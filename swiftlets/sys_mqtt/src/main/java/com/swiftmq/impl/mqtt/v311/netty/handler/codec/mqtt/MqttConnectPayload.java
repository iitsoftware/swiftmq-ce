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

import com.swiftmq.impl.mqtt.v311.netty.util.internal.StringUtil;

/**
 * Payload of {@link MqttConnectMessage}
 */
public final class MqttConnectPayload {

    private final String clientIdentifier;
    private final String willTopic;
    private final byte[] willMessage;
    private final String userName;
    private final byte[] password;

    /**
     * @deprecated use {@link MqttConnectPayload#MqttConnectPayload(String, String, byte[], String, byte[])} instead
     */
    @Deprecated
    public MqttConnectPayload(
            String clientIdentifier,
            String willTopic,
            String willMessage,
            String userName,
            String password) {
        this(
                clientIdentifier,
                willTopic,
                willMessage.getBytes(com.swiftmq.impl.mqtt.v311.netty.util.internal.StringUtil.UTF8),
                userName,
                password.getBytes(com.swiftmq.impl.mqtt.v311.netty.util.internal.StringUtil.UTF8));
    }

    public MqttConnectPayload(
            String clientIdentifier,
            String willTopic,
            byte[] willMessage,
            String userName,
            byte[] password) {
        this.clientIdentifier = clientIdentifier;
        this.willTopic = willTopic;
        this.willMessage = willMessage;
        this.userName = userName;
        this.password = password;
    }

    public String clientIdentifier() {
        return clientIdentifier;
    }

    public String willTopic() {
        return willTopic;
    }

    /**
     * @deprecated use {@link MqttConnectPayload#willMessageInBytes()} instead
     */
    @Deprecated
    public String willMessage() {
        return willMessage == null ? null : new String(willMessage, com.swiftmq.impl.mqtt.v311.netty.util.internal.StringUtil.UTF8);
    }

    public byte[] willMessageInBytes() {
        return willMessage;
    }

    public String userName() {
        return userName;
    }

    /**
     * @deprecated use {@link MqttConnectPayload#passwordInBytes()} instead
     */
    @Deprecated
    public String password() {
        return password == null ? null : new String(password, com.swiftmq.impl.mqtt.v311.netty.util.internal.StringUtil.UTF8);
    }

    public byte[] passwordInBytes() {
        return password;
    }

    @Override
    public String toString() {
        return new StringBuilder(StringUtil.simpleClassName(this))
                .append('[')
                .append("clientIdentifier=").append(clientIdentifier)
                .append(", willTopic=").append(willTopic)
                .append(", willMessage=").append(willMessage)
                .append(", userName=").append(userName)
                .append(", password=").append(password)
                .append(']')
                .toString();
    }
}
