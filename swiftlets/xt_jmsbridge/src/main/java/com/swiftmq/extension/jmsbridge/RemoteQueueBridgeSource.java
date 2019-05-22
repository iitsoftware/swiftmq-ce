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

package com.swiftmq.extension.jmsbridge;

import javax.jms.*;

public class RemoteQueueBridgeSource implements BridgeSource {
    SwiftletContext ctx = null;
    String tracePrefix = null;
    Queue queue = null;
    QueueSession session = null;
    QueueReceiver receiver = null;
    BridgeSink sink = null;
    ErrorListener errorListener = null;

    RemoteQueueBridgeSource(SwiftletContext ctx, String tracePrefix, QueueConnection connection, Queue queue) throws Exception {
        this.ctx = ctx;
        this.queue = queue;
        this.tracePrefix = tracePrefix + "/" + toString();
        session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        receiver = session.createReceiver(queue);
    }

    public void setBridgeSink(BridgeSink sink) {
        this.sink = sink;
    }

    public void setErrorListener(ErrorListener l) {
        this.errorListener = l;
    }

    public void startDelivery() throws Exception {
        receiver.setMessageListener(new MessageListener() {
            public void onMessage(Message msg) {
                ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
                try {
                    Thread.currentThread().setContextClassLoader(BridgeSource.class.getClassLoader());
                    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "onMessage(): " + msg);
                    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "putMessage() ...");
                    sink.putMessage(msg);
                    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "After putMessage(): " + msg);
                    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "commiting sink ...");
                    sink.commit();
                } catch (Exception e) {
                    try {
                        receiver.setMessageListener(null);
                    } catch (Exception ignored) {
                    }
                    if (errorListener != null)
                        errorListener.onError(e);
                }
                Thread.currentThread().setContextClassLoader(oldCL);
            }
        });
    }

    /**
     * @throws Exception
     */
    public void destroy() {
        try {
            receiver.setMessageListener(null);
        } catch (Exception ignored) {
        }
        try {
            receiver.close();
        } catch (Exception ignored) {
        }
        try {
            session.close();
        } catch (Exception ignored) {
        }
    }

    public String toString() {
        String name = null;
        try {
            name = queue.getQueueName();
        } catch (Exception ignored) {
        }
        return "[RemoteQueueBridgeSource, queue=" + name + "]";
    }
}

