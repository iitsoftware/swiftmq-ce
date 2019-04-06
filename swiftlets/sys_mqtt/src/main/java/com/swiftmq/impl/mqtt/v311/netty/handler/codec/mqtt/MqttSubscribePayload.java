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

import java.util.Collections;
import java.util.List;

/**
 * Payload of the {@link MqttSubscribeMessage}
 */
public final class MqttSubscribePayload {

    private final List<MqttTopicSubscription> topicSubscriptions;

    public MqttSubscribePayload(List<MqttTopicSubscription> topicSubscriptions) {
        this.topicSubscriptions = Collections.unmodifiableList(topicSubscriptions);
    }

    public List<MqttTopicSubscription> topicSubscriptions() {
        return topicSubscriptions;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this)).append('[');
        for (int i = 0; i < topicSubscriptions.size() - 1; i++) {
            builder.append(topicSubscriptions.get(i)).append(", ");
        }
        builder.append(topicSubscriptions.get(topicSubscriptions.size() - 1));
        builder.append(']');
        return builder.toString();
    }
}
