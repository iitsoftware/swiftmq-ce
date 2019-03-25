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
import com.swiftmq.mqtt.v311.netty.handler.codec.mqtt.MqttEncoder;
import com.swiftmq.mqtt.v311.netty.handler.codec.mqtt.MqttMessage;
import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.tools.queue.SingleProcessorQueue;
import com.swiftmq.tools.util.DataStreamOutputStream;

public class OutboundQueue extends SingleProcessorQueue {
    SwiftletContext ctx;
    ThreadPool pool;
    MQTTConnection connection;
    QueueProcessor queueProcessor;
    DataStreamOutputStream dos = null;

    OutboundQueue(SwiftletContext ctx, ThreadPool pool, MQTTConnection connection) {
        super(100);
        this.ctx = ctx;
        this.pool = pool;
        this.connection = connection;
        dos = new DataStreamOutputStream(connection.getConnection().getOutputStream());
        queueProcessor = new QueueProcessor();
    }

    protected void startProcessor() {
        if (!connection.closed)
            pool.dispatchTask(queueProcessor);
    }

    protected void process(Object[] bulk, int n) {
        try {
            for (int i = 0; i < n; i++) {
                MqttEncoder.doEncode((MqttMessage) bulk[i]).flushTo(dos);
                if (ctx.protSpace.enabled)
                    ctx.protSpace.trace("mqtt", connection.toString() + "/SND: " + bulk[i]);
            }
            dos.flush();
        } catch (Exception e) {
            connection.close();
        }
    }

    private class QueueProcessor implements AsyncTask {
        public boolean isValid() {
            return !connection.closed;
        }

        public String getDispatchToken() {
            return MQTTConnection.TP_CONNECTIONSVC;
        }

        public String getDescription() {
            return connection.toString() + "/QueueProcessor";
        }

        public void stop() {
        }

        public void run() {
            if (!connection.closed && dequeue())
                pool.dispatchTask(this);
        }
    }
}
