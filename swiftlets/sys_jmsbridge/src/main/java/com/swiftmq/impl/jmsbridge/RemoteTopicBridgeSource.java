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

package com.swiftmq.impl.jmsbridge;

import javax.jms.*;

public class RemoteTopicBridgeSource implements BridgeSource {
    SwiftletContext ctx = null;
    String tracePrefix = null;
    String durName = null;
    Topic topic = null;
    TopicSession session = null;
    TopicSubscriber subscriber = null;
    BridgeSink sink = null;
    ErrorListener errorListener = null;

    RemoteTopicBridgeSource(SwiftletContext ctx, String tracePrefix, TopicConnection connection, Topic topic, String durName) throws Exception {
        this.ctx = ctx;
        this.topic = topic;
        this.tracePrefix = tracePrefix + "/" + toString();
        this.durName = durName;
        session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        if (durName != null && durName.trim().length() > 0)
            subscriber = session.createDurableSubscriber(topic, durName);
        else
            subscriber = session.createSubscriber(topic);
    }

    public void setBridgeSink(BridgeSink sink) {
        this.sink = sink;
    }

    public void setErrorListener(ErrorListener l) {
        this.errorListener = l;
    }

    public void startDelivery() throws Exception {
        subscriber.setMessageListener(new MessageListener() {
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
                        subscriber.setMessageListener(null);
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
            subscriber.setMessageListener(null);
        } catch (Exception ignored) {
        }
        try {
            subscriber.close();
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
            name = topic.getTopicName();
        } catch (Exception ignored) {
        }
        return "[RemoteTopicBridgeSource, topic=" + name + "]";
    }
}

