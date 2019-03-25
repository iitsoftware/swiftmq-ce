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

package com.swiftmq.impl.mqtt.retain;

import com.swiftmq.impl.mqtt.SwiftletContext;
import com.swiftmq.mqtt.v311.netty.buffer.ByteBuf;
import com.swiftmq.mqtt.v311.netty.handler.codec.mqtt.MqttPublishMessage;
import com.swiftmq.tools.sql.LikeComparator;

import java.util.*;

public class MemoryRetainer implements Retainer {
    SwiftletContext ctx;
    Map<String, MqttPublishMessage> messages = new HashMap<String, MqttPublishMessage>();

    public MemoryRetainer(SwiftletContext ctx) {
        this.ctx = ctx;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + "/created");
    }

    @Override
    public synchronized void add(String mqttTopic, MqttPublishMessage message) {
        ByteBuf payload = message.payload();
        if (payload.size() == 0) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + "/add, topic=" + mqttTopic + ", zero byte payload, remove retained message");
            messages.remove(mqttTopic);
        } else
            messages.put(mqttTopic, message);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + "/add, topic=" + mqttTopic + ", size=" + messages.size());
    }

    @Override
    public synchronized List<MqttPublishMessage> get(String mqttTopic) {
        List<MqttPublishMessage> result = new ArrayList<MqttPublishMessage>();
        for (Iterator<Map.Entry<String, MqttPublishMessage>> iter = messages.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<String, MqttPublishMessage> entry = iter.next();
            String topic = entry.getKey();
            if (LikeComparator.compare(topic, mqttTopic, '\\'))
                result.add(entry.getValue());
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + "/get, topic=" + mqttTopic + ", size=" + result.size());
        return result;
    }

    @Override
    public synchronized void close() {
        messages.clear();
    }

    @Override
    public String toString() {
        return "MemoryRetainer";
    }
}
