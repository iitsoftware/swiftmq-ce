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

package com.swiftmq.impl.queue.standard.jobs;

import com.swiftmq.impl.queue.standard.SwiftletContext;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.scheduler.Job;
import com.swiftmq.swiftlet.scheduler.JobException;
import com.swiftmq.swiftlet.scheduler.JobTerminationListener;
import com.swiftmq.tools.sql.LikeComparator;

import java.util.Properties;

public class QueueCleanupJob implements Job {
    SwiftletContext ctx = null;
    boolean stopCalled = false;
    Properties properties = null;

    public QueueCleanupJob(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    private void terminate() {
    }

    public void start(Properties properties, JobTerminationListener jobTerminationListener) throws JobException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/start, properties=" + properties + " ...");
        this.properties = properties;
        if (stopCalled) {
            terminate();
            return;
        }
        try {
            String predicate = properties.getProperty("Queue Name Predicate");
            String[] names = ctx.queueManager.getDefinedQueueNames();
            if (names != null) {
                for (int i = 0; i < names.length; i++) {
                    if (!names[i].startsWith("tpc$")) {
                        if (LikeComparator.compare(names[i], predicate, '\\')) {
                            try {
                                AbstractQueue queue = ctx.queueManager.getQueueForInternalUse(names[i]);
                                if (queue != null) {
                                    if (ctx.traceSpace.enabled)
                                        ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/cleanup: " + names[i]);
                                    queue.cleanUpExpiredMessages();
                                }
                            } catch (QueueException e) {
                                if (ctx.traceSpace.enabled)
                                    ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/cleanup: " + names[i] + ", exception=" + e);
                            }
                        }
                    }
                    if (stopCalled)
                        break;
                }
            }
        } catch (Exception e) {
            terminate();
            throw new JobException(e.toString(), e, false);
        }
        terminate();
        jobTerminationListener.jobTerminated();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/start, properties=" + properties + " done");
    }

    public void stop() throws JobException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/stop ...");
        stopCalled = true;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/stop done");
    }

    public String toString() {
        return "[QueueCleanupJob, properties=" + properties + "]";
    }
}
