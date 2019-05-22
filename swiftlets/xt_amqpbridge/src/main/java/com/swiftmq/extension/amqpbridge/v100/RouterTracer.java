package com.swiftmq.extension.amqpbridge.v100;

import com.swiftmq.amqp.integration.Tracer;
import com.swiftmq.swiftlet.trace.TraceSpace;

public class RouterTracer extends Tracer {
    TraceSpace traceSpace = null;

    public RouterTracer(TraceSpace traceSpace) {
        this.traceSpace = traceSpace;
    }

    public boolean isEnabled() {
        return traceSpace.enabled;
    }

    public void trace(String key, String msg) {
        traceSpace.trace(key, msg);
    }
}
