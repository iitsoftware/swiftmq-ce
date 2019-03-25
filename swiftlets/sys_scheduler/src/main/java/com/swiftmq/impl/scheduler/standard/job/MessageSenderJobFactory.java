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

package com.swiftmq.impl.scheduler.standard.job;

import com.swiftmq.impl.scheduler.standard.SwiftletContext;
import com.swiftmq.swiftlet.scheduler.Job;
import com.swiftmq.swiftlet.scheduler.JobException;
import com.swiftmq.swiftlet.scheduler.JobFactory;
import com.swiftmq.swiftlet.scheduler.JobParameter;

import java.util.HashMap;
import java.util.Map;

public class MessageSenderJobFactory implements JobFactory {
    SwiftletContext ctx = null;
    Map parms = new HashMap();

    public MessageSenderJobFactory(SwiftletContext ctx) {
        this.ctx = ctx;
        JobParameter p = new JobParameter(ctx.PARM, "Schedule ID", null, true, null);
        parms.put(p.getName(), p);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/created");
    }

    public String getName() {
        return ctx.JOBNAME;
    }

    public String getDescription() {
        return ctx.JOBNAME;
    }

    public Map getJobParameters() {
        return parms;
    }

    public JobParameter getJobParameter(String s) {
        return (JobParameter) parms.get(s);
    }

    public Job getJobInstance() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/getJobInstance");
        return new MessageSenderJob(ctx);
    }

    public void finished(Job job, JobException e) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/finished, job=" + job + ", jobException=" + e);
    }

    public String toString() {
        return "[MessageSenderJobFactory]";
    }
}
