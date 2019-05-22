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
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.swiftlet.threadpool.ThreadPool;

public class LocalQueueBridgeSource implements BridgeSource {
    // Bridge-Threadpool names
    static final String TP_BRIDGE = "extension.xt$bridge.source.queue";

    SwiftletContext ctx = null;
    String tracePrefix = null;
    ThreadPool myTP = null;
    String queueName = null;
    QueueReceiver receiver = null;
    QueuePullTransaction pullTransaction = null;
    ErrorListener errorListener = null;
    BridgeSink sink = null;
    SourceMessageProcessor msgProc = null;

    LocalQueueBridgeSource(SwiftletContext ctx, String tracePrefix, String queueName) throws Exception {
        this.ctx = ctx;
        this.queueName = queueName;
        this.tracePrefix = tracePrefix + "/" + toString();
        myTP = ctx.threadpoolSwiftlet.getPool(TP_BRIDGE);
        QueueManager queueManager = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
        receiver = queueManager.createQueueReceiver(queueName, null, null);
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
    }

    public String toString() {
        return "[LocalQueueBridgeSource, queue=" + queueName + "]";
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
            return "xt$bridge/" + LocalQueueBridgeSource.this.toString();
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

