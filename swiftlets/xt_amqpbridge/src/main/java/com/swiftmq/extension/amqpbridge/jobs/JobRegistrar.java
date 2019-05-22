package com.swiftmq.extension.amqpbridge.jobs;

import com.swiftmq.extension.amqpbridge.SwiftletContext;
import com.swiftmq.swiftlet.scheduler.JobFactory;
import com.swiftmq.swiftlet.scheduler.JobGroup;

public class JobRegistrar {
    SwiftletContext ctx = null;
    JobGroup jobGroup = null;

    public JobRegistrar(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    public void register() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/register ...");
        jobGroup = ctx.schedulerSwiftlet.getJobGroup("AMQP Bridge");
        JobFactory jf = new BridgeJobFactory(ctx);
        jobGroup.addJobFactory(jf.getName(), jf);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/register done");
    }

    public void unregister() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/unregister ...");
        jobGroup.removeAll();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/unregister done");
    }
}
