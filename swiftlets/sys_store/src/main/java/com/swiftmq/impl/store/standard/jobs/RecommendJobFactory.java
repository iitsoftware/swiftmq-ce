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

package com.swiftmq.impl.store.standard.jobs;

import com.swiftmq.impl.store.standard.StoreContext;
import com.swiftmq.swiftlet.scheduler.Job;
import com.swiftmq.swiftlet.scheduler.JobException;
import com.swiftmq.swiftlet.scheduler.JobFactory;
import com.swiftmq.swiftlet.scheduler.JobParameter;

import java.util.HashMap;
import java.util.Map;

public class RecommendJobFactory implements JobFactory {
    StoreContext ctx = null;
    Map parameters = new HashMap();

    public RecommendJobFactory(StoreContext ctx) {
        this.ctx = ctx;
    }

    public String getName() {
        return "Recommend";
    }

    public String getDescription() {
        return "Scans page.db and recommends a new page size";
    }

    public Map getJobParameters() {
        return parameters;
    }

    public JobParameter getJobParameter(String s) {
        return (JobParameter) parameters.get(s);
    }

    public Job getJobInstance() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/getJobInstance");
        return new RecommendJob(ctx);
    }

    public void finished(Job job, JobException e) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/finished, job=" + job + ", jobException=" + e);
    }

    public String toString() {
        return "[RecommendJobFactory]";
    }
}