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
import com.swiftmq.swiftlet.scheduler.JobFactory;
import com.swiftmq.swiftlet.scheduler.JobGroup;

public class JobRegistrar {
    SwiftletContext ctx = null;
    JobGroup jobGroup = null;

    public JobRegistrar(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    public void register() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/register ...");
        jobGroup = ctx.schedulerSwiftlet.getJobGroup("Queue Manager");
        JobFactory jf = new QueuePurgerJobFactory(ctx);
        jobGroup.addJobFactory(jf.getName(), jf);
        jf = new MultiQueuePurgerJobFactory(ctx);
        jobGroup.addJobFactory(jf.getName(), jf);
        jf = new QueueMoverJobFactory(ctx);
        jobGroup.addJobFactory(jf.getName(), jf);
        jf = new QueueCleanupJobFactory(ctx);
        jobGroup.addJobFactory(jf.getName(), jf);
        jf = new QueueCleanupDLQJobFactory(ctx);
        jobGroup.addJobFactory(jf.getName(), jf);
        jf = new QueueResetJobFactory(ctx);
        jobGroup.addJobFactory(jf.getName(), jf);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/register done");
    }

    public void unregister() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/unregister ...");
        jobGroup.removeAll();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/unregister done");
    }
}
