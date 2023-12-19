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

package com.swiftmq.impl.queue.standard.cluster;

import com.swiftmq.impl.queue.standard.QueueManagerImpl;
import com.swiftmq.impl.queue.standard.SwiftletContext;
import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.swiftlet.queue.MessageProcessor;
import com.swiftmq.swiftlet.queue.QueuePullTransaction;
import com.swiftmq.swiftlet.queue.QueueReceiver;
import com.swiftmq.tools.versioning.Versionable;

import java.util.List;

public class ClusterMetricSubscriber extends MessageProcessor {
    SwiftletContext ctx = null;
    volatile String queueName = null;
    volatile QueueReceiver receiver = null;
    volatile int subscriberId = 0;
    volatile QueuePullTransaction t = null;
    volatile boolean closed = false;
    volatile BytesMessageImpl msg = null;

    public ClusterMetricSubscriber(SwiftletContext ctx) throws Exception {
        this.ctx = ctx;
        queueName = ctx.queueManager.createTemporaryQueue();
        receiver = ctx.queueManager.createQueueReceiver(queueName, null, null);
        subscriberId = ctx.topicManager.subscribe(QueueManagerImpl.CLUSTER_TOPIC, null, false, queueName);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/created");
        t = receiver.createTransaction(false);
        t.registerMessageProcessor(this);
    }

    public boolean isValid() {
        return !closed;
    }

    public void processException(Exception exception) {
        if (closed)
            return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/processException: " + exception + ", EXITING!");
    }

    public void processMessage(MessageEntry messageEntry) {
        try {
            msg = (BytesMessageImpl) messageEntry.getMessage();
            msg.reset();
            if (msg.getStringProperty(ClusterMetricPublisher.PROP_SOURCE_ROUTER).equals(SwiftletManager.getInstance().getRouterName())) {
                t.commit();
                if (!closed) {
                    t = receiver.createTransaction(false);
                    t.registerMessageProcessor(this);
                }
                return;
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/received: " + msg);
            ctx.threadpoolSwiftlet.runAsync(this);
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/exception occurred: " + e + ", EXITING!");
        }
    }

    public void run() {
        try {
            t.commit();
            try {
                Versionable versionable = Versionable.toVersionable(msg);
                versionable.setClassLoader(getClass().getClassLoader());
                int version = versionable.selectVersions(QueueManagerImpl.VERSIONS);
                switch (version) {
                    case 700: {
                        ClusteredQueueMetricCollection cmc = (ClusteredQueueMetricCollection) versionable.createVersionedObject();
                        List list = cmc.getClusteredQueueMetrics();
                        if (list != null) {
                            for (Object o : list) {
                                ClusteredQueueMetric cm = (ClusteredQueueMetric) o;
                                if (ctx.traceSpace.enabled)
                                    ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/run, cm=" + cm);
                                DispatchPolicy dp = ctx.dispatchPolicyRegistry.get(cm.getClusteredQueueName());
                                if (dp != null)
                                    dp.addMetric(cmc.getRouterName(), cm);
                            }
                        }
                    }
                    break;
                    default:
                        throw new Exception("Invalid version: " + version);
                }
            } catch (Exception e) {
                if (closed)
                    return;
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/exception occurred: " + e);
                ctx.logSwiftlet.logError(ctx.queueManager.getName(), toString() + "/exception occurred: " + e);
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
                ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/exception occurred: " + e + ", EXITING!");
        }
    }

    public void close() {
        closed = true;
    }

    public String toString() {
        return "ClusterMetricSubscriber";
    }
}
