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

package com.swiftmq.impl.jmsbridge.jobs;

import com.swiftmq.impl.jmsbridge.SwiftletContext;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.swiftlet.scheduler.Job;
import com.swiftmq.swiftlet.scheduler.JobException;
import com.swiftmq.swiftlet.scheduler.JobFactory;
import com.swiftmq.swiftlet.scheduler.JobParameter;

import java.util.HashMap;
import java.util.Map;

public class ServerBridgeJobFactory implements JobFactory {
    SwiftletContext ctx = null;
    EntityList entityList = null;
    Map parameters = new HashMap();

    public ServerBridgeJobFactory(SwiftletContext ctx, EntityList entityList) {
        this.ctx = ctx;
        this.entityList = entityList;
        JobParameter p = new JobParameter("Bridge Name", "Name of the Server Bridge", null, true, null);
        parameters.put(p.getName(), p);
    }

    public String getName() {
        return "Server Bridge";
    }

    public String getDescription() {
        return "Activates a Server Bridge";
    }

    public Map getJobParameters() {
        return parameters;
    }

    public JobParameter getJobParameter(String s) {
        return (JobParameter) parameters.get(s);
    }

    public Job getJobInstance() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("xt$bridge", toString() + "/getJobInstance");
        return new ServerBridgeJob(ctx, entityList);
    }

    public void finished(Job job, JobException e) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("xt$bridge", toString() + "/finished, job=" + job + ", jobException=" + e);
    }

    public String toString() {
        return "[ServerBridgeJobFactory]";
    }
}
