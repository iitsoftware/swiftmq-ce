package com.swiftmq.extension.amqpbridge.jobs;

import com.swiftmq.extension.amqpbridge.SwiftletContext;
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
