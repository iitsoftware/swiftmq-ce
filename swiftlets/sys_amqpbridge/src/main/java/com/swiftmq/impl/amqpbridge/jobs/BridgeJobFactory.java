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

package com.swiftmq.impl.amqpbridge.jobs;

import com.swiftmq.impl.amqpbridge.SwiftletContext;
import com.swiftmq.swiftlet.scheduler.*;

import java.util.HashMap;
import java.util.Map;

public class BridgeJobFactory implements JobFactory {
    SwiftletContext ctx = null;
    Map parameters = new HashMap();

    public BridgeJobFactory(SwiftletContext ctx) {
        this.ctx = ctx;
        JobParameter p = new JobParameter("Bridge Name", "Name of the Bridge", null, true, null);
        parameters.put(p.getName(), p);
        p = new JobParameter("Bridge Type", "Type of the Bridge (091 or 100)", null, true, new JobParameterVerifier() {
            public void verify(JobParameter jobParameter, String s) throws InvalidValueException {
                if (s == null || !(s.equals("091") || s.equals("100")))
                    throw new InvalidValueException("Value must be 091 or 100");
            }
        }
        );
        parameters.put(p.getName(), p);
    }

    public String getName() {
        return "Bridge";
    }

    public String getDescription() {
        return "Activates a Bridge";
    }

    public Map getJobParameters() {
        return parameters;
    }

    public JobParameter getJobParameter(String s) {
        return (JobParameter) parameters.get(s);
    }

    public Job getJobInstance() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/getJobInstance");
        return new BridgeJob(ctx);
    }

    public void finished(Job job, JobException e) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/finished, job=" + job + ", jobException=" + e);
    }

    public String toString() {
        return "[BridgeJobFactory]";
    }
}
