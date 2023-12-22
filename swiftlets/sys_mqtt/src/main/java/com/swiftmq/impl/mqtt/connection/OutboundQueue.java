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

package com.swiftmq.impl.mqtt.connection;

import com.swiftmq.impl.mqtt.SwiftletContext;
import com.swiftmq.impl.mqtt.v311.netty.handler.codec.mqtt.MqttEncoder;
import com.swiftmq.impl.mqtt.v311.netty.handler.codec.mqtt.MqttMessage;
import com.swiftmq.swiftlet.threadpool.EventLoop;
import com.swiftmq.tools.util.DataStreamOutputStream;

public class OutboundQueue {
    SwiftletContext ctx;
    MQTTConnection connection;
    DataStreamOutputStream dos = null;
    EventLoop eventLoop;

    public OutboundQueue(SwiftletContext ctx, MQTTConnection connection) {
        this.ctx = ctx;
        this.connection = connection;
        dos = new DataStreamOutputStream(connection.getConnection().getOutputStream());
        eventLoop = ctx.threadpoolSwiftlet.createEventLoop("sys$mqtt.connection.outbound", list -> {
            try {
                for (Object event : list) {
                    MqttEncoder.doEncode((MqttMessage) event).flushTo(dos);
                    if (ctx.protSpace.enabled)
                        ctx.protSpace.trace("mqtt", connection.toString() + "/SND: " + event);
                }
                dos.flush();
            } catch (Exception e) {
                connection.close();
            }
        });
    }

    public void submit(Object event) {
        eventLoop.submit(event);
    }

    void close() {
        eventLoop.close();
    }


}
