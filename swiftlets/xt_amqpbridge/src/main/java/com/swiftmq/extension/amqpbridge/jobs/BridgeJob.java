package com.swiftmq.extension.amqpbridge.jobs;

import com.swiftmq.extension.amqpbridge.SwiftletContext;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.scheduler.Job;
import com.swiftmq.swiftlet.scheduler.JobException;
import com.swiftmq.swiftlet.scheduler.JobTerminationListener;

import java.util.Properties;

public class BridgeJob implements Job {
    SwiftletContext ctx = null;
    Properties properties = null;
    String name = null;

    public BridgeJob(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    private void doAction(EntityList entityList, boolean b) throws JobException {
        Entity entity = entityList.getEntity(name);
        if (entity == null)
            throw new JobException("Bridge '" + name + "' is undefined!", null, false);
        Property prop = entity.getProperty("enabled");
        boolean enabled = ((Boolean) prop.getValue()).booleanValue();
        try {
            if (enabled != b)
                prop.setValue(new Boolean(b));
        } catch (Exception e) {
            throw new JobException(e.getMessage(), e, false);
        }
    }

    public void start(Properties properties, JobTerminationListener jobTerminationListener) throws JobException {
        this.properties = properties;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/start ...");
        name = properties.getProperty("Bridge Name");
        doAction((EntityList) (properties.getProperty("Bridge Type").equals("091") ? ctx.root.getEntity("bridges-091") : ctx.root.getEntity("bridges-100")), true);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/start done");
    }

    public void stop() throws JobException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/stop ...");
        doAction((EntityList) (properties.getProperty("Bridge Type").equals("091") ? ctx.root.getEntity("bridges-091") : ctx.root.getEntity("bridges-100")), false);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/stop done");
    }

    public String toString() {
        return "[BridgeJob, properties=" + properties + "]";
    }
}
