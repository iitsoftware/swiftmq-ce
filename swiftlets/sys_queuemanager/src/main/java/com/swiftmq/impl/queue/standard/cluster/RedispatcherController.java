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

package com.swiftmq.impl.queue.standard.cluster;

import com.swiftmq.impl.queue.standard.SwiftletContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedispatcherController {
    SwiftletContext ctx = null;
    Map<String, Redispatcher> redispatchers = new ConcurrentHashMap<>();

    public RedispatcherController(SwiftletContext ctx) {
        this.ctx = ctx;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/stop");
    }

    public void redispatch(String sourceQueueName, String targetQueueName) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/redispatch, source=" + sourceQueueName + ", target=" + targetQueueName + " ...");

        Redispatcher rdp = redispatchers.computeIfAbsent(sourceQueueName, key -> {
            try {
                return new Redispatcher(ctx, sourceQueueName, targetQueueName);
            } catch (Exception e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/redispatch, source=" + sourceQueueName + ", target=" + targetQueueName + ", exception=" + e);
                return null;
            }
        });

        if (rdp == null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/redispatch, source=" + sourceQueueName + ", target=" + targetQueueName + ", already running, do nothing!");
            return;
        }

        try {
            rdp.start();
        } catch (Exception e) {
            e.printStackTrace();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/redispatch, source=" + sourceQueueName + ", target=" + targetQueueName + ", exception=" + e);
        }

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/redispatch, source=" + sourceQueueName + ", target=" + targetQueueName + " done");
    }

    public void redispatcherFinished(String sourceQueueName) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/redispatcherFinished, source=" + sourceQueueName);
        redispatchers.remove(sourceQueueName);
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/close ...");

        for (Map.Entry<String, Redispatcher> entry : redispatchers.entrySet()) {
            entry.getValue().stop();
        }
        redispatchers.clear();

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/close done");
    }
    public String toString() {
        return "RedispatcherController";
    }
}
