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

import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.swiftlet.queue.MessageProcessor;
import com.swiftmq.swiftlet.queue.QueuePullTransaction;
import com.swiftmq.swiftlet.queue.QueueReceiver;
import com.swiftmq.swiftlet.threadpool.ThreadPool;

public class LocalTopicBridgeSource implements BridgeSource {
    // Bridge-Threadpool names
    static final String TP_BRIDGE = "extension.xt$bridge.source.topic";

    SwiftletContext ctx = null;
    ActiveLogin activeLogin = null;
    ThreadPool myTP = null;
    String tracePrefix = null;
    String topicName = null;
    String queueName = null;
    String clientId = null;
    String durName = null;
    QueueReceiver receiver = null;
    QueuePullTransaction pullTransaction = null;
    int subscriberId = -1;
    ErrorListener errorListener = null;
    BridgeSink sink = null;
    SourceMessageProcessor msgProc = null;

    LocalTopicBridgeSource(SwiftletContext ctx, String tracePrefix, String topicName, String clientId, String durName) throws Exception {
        this.ctx = ctx;
        this.topicName = topicName;
        this.tracePrefix = tracePrefix + "/" + toString();
        this.clientId = clientId;
        this.durName = durName;
        myTP = ctx.threadpoolSwiftlet.getPool(TP_BRIDGE);
        TopicImpl topic = ctx.topicManager.verifyTopic(new TopicImpl(topicName));
        if (durName != null && durName.trim().length() > 0) {
            activeLogin = ctx.authSwiftlet.createActiveLogin(clientId, "DURABLE");
            activeLogin.setClientId(clientId);
            queueName = ctx.topicManager.subscribeDurable(durName, topic, null, true, activeLogin);
        } else {
            queueName = ctx.queueManager.createTemporaryQueue();
            subscriberId = ctx.topicManager.subscribe(topicName, null, false, queueName);
        }

        receiver = ctx.queueManager.createQueueReceiver(queueName, null, null);
    }

    public void setBridgeSink(BridgeSink sink) {
        this.sink = sink;
    }

    public void setErrorListener(ErrorListener l) {
        this.errorListener = l;
    }

    public void startDelivery() throws Exception {
        pullTransaction = receiver.createTransaction(false);
        msgProc = new SourceMessageProcessor();
        pullTransaction.registerMessageProcessor(msgProc);
    }

    public void destroy() {
        if (msgProc != null)
            msgProc.setClosed();
        if (pullTransaction != null) {
            try {
                pullTransaction.rollback();
                pullTransaction = null;
            } catch (Exception ignored) {
            }
        }
        try {
            receiver.close();
        } catch (Exception ignored) {
        }
        if (subscriberId != -1) {
            try {
                ctx.topicManager.unsubscribe(subscriberId);
            } catch (Exception ignored) {
            }
        }
        try {
            ctx.queueManager.deleteTemporaryQueue(queueName);
        } catch (Exception ignored) {
        }
    }

    public String toString() {
        return "[LocalTopicBridgeSource, queue=" + queueName + ", topic=" + topicName + "]";
    }

    private class SourceMessageProcessor extends MessageProcessor {
        MessageImpl msg = null;
        boolean closed = false;

        void setClosed() {
            closed = true;
        }

        public boolean isValid() {
            return !closed;
        }

        public void processMessage(MessageEntry messageEntry) {
            try {
                msg = messageEntry.getMessage();
                myTP.dispatchTask(this);
            } catch (Exception e) {
                errorListener.onError(e);
            }
        }

        public void processException(Exception exception) {
            errorListener.onError(exception);
        }

        public String getDispatchToken() {
            return TP_BRIDGE;
        }

        public String getDescription() {
            return "xt$bridge/" + LocalTopicBridgeSource.this.toString();
        }

        public void stop() {
            setClosed();
        }

        public void run() {
            ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(BridgeSource.class.getClassLoader());
                if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "processing: " + msg);
                if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "putMessage() ...");
                sink.putMessage(msg);
                if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "After putMessage(): " + msg);
                if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "commiting sink ...");
                sink.commit();
                if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "commiting source ...");
                pullTransaction.commit();
                pullTransaction = receiver.createTransaction(false);
                pullTransaction.registerMessageProcessor(this);
            } catch (Exception e) {
                errorListener.onError(e);
            }
            Thread.currentThread().setContextClassLoader(oldCL);
        }
    }
}

