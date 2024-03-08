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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class RedispatcherController {
    SwiftletContext ctx = null;
    Map<String, Redispatcher> redispatchers = new HashMap<>();
    ReentrantLock lock = new ReentrantLock();

    public RedispatcherController(SwiftletContext ctx) {
        this.ctx = ctx;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), this + "/stop");
    }

    public void redispatch(String sourceQueueName, String targetQueueName) {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/redispatch, source=" + sourceQueueName + ", target=" + targetQueueName + " ...");
            if (redispatchers.get(sourceQueueName) != null) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/redispatch, source=" + sourceQueueName + ", target=" + targetQueueName + ", already running, do nothing!");
                return;
            }
            try {
                Redispatcher rdp = new Redispatcher(ctx, sourceQueueName, targetQueueName);
                redispatchers.put(sourceQueueName, rdp);
                rdp.start();
            } catch (Exception e) {
                e.printStackTrace();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/redispatch, source=" + sourceQueueName + ", target=" + targetQueueName + ", exception=" + e);
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/redispatch, source=" + sourceQueueName + ", target=" + targetQueueName + " done");
        } finally {
            lock.unlock();
        }

    }

    public void redispatcherFinished(String sourceQueueName) {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.queueManager.getName(), this + "/redispatcherFinished, source=" + sourceQueueName);
            redispatchers.remove(sourceQueueName);
        } finally {
            lock.unlock();
        }

    }

    public void close() {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), this + "/close ...");

            for (Map.Entry<String, Redispatcher> entry : redispatchers.entrySet()) {
                entry.getValue().stop();
            }
            redispatchers.clear();

            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), this + "/close done");
        } finally {
            lock.unlock();
        }

    }
    public String toString() {
        return "RedispatcherController";
    }
}
