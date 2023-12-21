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

package com.swiftmq.impl.jndi.standard;

import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jndi.protocol.v400.JNDIRequest;
import com.swiftmq.swiftlet.jndi.JNDISwiftlet;
import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.swiftlet.queue.MessageProcessor;
import com.swiftmq.swiftlet.queue.QueuePullTransaction;
import com.swiftmq.swiftlet.queue.QueueReceiver;
import com.swiftmq.tools.versioning.Versionable;

public class TopicJNDIProcessor extends MessageProcessor {
    SwiftletContext ctx = null;
    volatile String queueName = null;
    volatile QueueReceiver receiver = null;
    volatile int subscriberId = 0;
    volatile QueuePullTransaction t = null;
    volatile BytesMessageImpl msg = null;
    volatile boolean closed = false;

    TopicJNDIProcessor(SwiftletContext ctx) throws Exception {
        this.ctx = ctx;
        queueName = ctx.queueManager.createTemporaryQueue();
        receiver = ctx.queueManager.createQueueReceiver(queueName, null, null);
        subscriberId = ctx.topicManager.subscribe(JNDISwiftlet.JNDI_TOPIC, null, false, queueName);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "Starting TopicJNDIProcessor...");
        t = receiver.createTransaction(false);
        t.registerMessageProcessor(this);
    }

    public boolean isValid() {
        return true;
    }

    public void processMessage(MessageEntry messageEntry) {
        try {
            msg = (BytesMessageImpl) messageEntry.getMessage();
            msg.reset();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "TopicJNDIProcessor: receiving request: " + msg);
            ctx.threadpoolSwiftlet.runAsync(this);
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "TopicJNDIProcessor: exception occurred: " + e + ", EXITING!");
        }
    }

    public void processException(Exception exception) {
        if (closed)
            return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "TopicJNDIProcessor: processException: " + exception + ", EXITING!");
    }

    public String getDispatchToken() {
        return "none";
    }

    public String getDescription() {
        return ctx.jndiSwiftlet.getName() + "/RequestProcessor";
    }

    public void stop() {
    }

    public void run() {
        try {
            t.commit();
            try {
                Versionable versionable = Versionable.toVersionable(msg);
                int version = versionable.selectVersions(JNDISwiftletImpl.VERSIONS);
                switch (version) {
                    case 400: {
                        JNDIRequest r = (JNDIRequest) versionable.createVersionedObject();
                        com.swiftmq.impl.jndi.standard.v400.RequestVisitor visitor = new com.swiftmq.impl.jndi.standard.v400.RequestVisitor(ctx, (QueueImpl) msg.getJMSReplyTo());
                        r.accept(visitor);
                    }
                    break;
                    default:
                        throw new Exception("Invalid version: " + version);
                }
            } catch (Exception e) {
                if (closed)
                    return;
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "TopicJNDIProcessor: exception occurred: " + e);
                ctx.logSwiftlet.logError(ctx.jndiSwiftlet.getName(), "TopicJNDIProcessor: exception occurred: " + e);
            }
            msg = null;
            if (closed)
                return;
            t = receiver.createTransaction(false);
            t.registerMessageProcessor(this);
        } catch (Exception e) {
            if (closed)
                return;
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "TopicJNDIProcessor: exception occurred: " + e + ", EXITING!");
        }
    }

    public void close() {
        closed = true;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "TopicJNDIProcessor: close");
        try {
            ctx.topicManager.unsubscribe(subscriberId);
        } catch (Exception ignored) {
        }
        try {
            if (t != null)
                t.rollback();
        } catch (Exception ignored) {
        }
        try {
            receiver.close();
        } catch (Exception ignored) {
        }
        try {
            ctx.queueManager.deleteTemporaryQueue(queueName);
        } catch (Exception ignored) {
        }

    }
}
