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

package com.swiftmq.impl.topic.standard.jobs;

import com.swiftmq.impl.topic.standard.TopicManagerContext;
import com.swiftmq.impl.topic.standard.TopicManagerImpl;
import com.swiftmq.swiftlet.scheduler.JobFactory;
import com.swiftmq.swiftlet.scheduler.JobGroup;
import com.swiftmq.swiftlet.scheduler.SchedulerSwiftlet;

public class JobRegistrar {
    TopicManagerImpl topicManager = null;
    SchedulerSwiftlet schedulerSwiftlet = null;
    TopicManagerContext ctx = null;
    JobGroup jobGroup = null;

    public JobRegistrar(TopicManagerImpl topicManager, SchedulerSwiftlet schedulerSwiftlet, TopicManagerContext ctx) {
        this.topicManager = topicManager;
        this.schedulerSwiftlet = schedulerSwiftlet;
        this.ctx = ctx;
    }

    public void register() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(topicManager.getName(), toString() + "/register ...");
        jobGroup = schedulerSwiftlet.getJobGroup("Topic Manager");
        JobFactory jf = new DeleteDurableJobFactory(topicManager, ctx.traceSpace, ctx.activeDurableList);
        jobGroup.addJobFactory(jf.getName(), jf);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(topicManager.getName(), toString() + "/register done");
    }

    public void unregister() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(topicManager.getName(), toString() + "/unregister ...");
        jobGroup.removeAll();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(topicManager.getName(), toString() + "/unregister done");
    }
}
