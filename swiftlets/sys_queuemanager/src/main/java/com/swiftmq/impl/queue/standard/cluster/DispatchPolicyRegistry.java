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
import com.swiftmq.impl.queue.standard.cluster.v700.ClusteredQueueMetricCollectionImpl;
import com.swiftmq.swiftlet.SwiftletManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DispatchPolicyRegistry {
    SwiftletContext ctx = null;
    Map<String, DispatchPolicy> policies = new ConcurrentHashMap<>();

    public DispatchPolicyRegistry(SwiftletContext ctx) {
        this.ctx = ctx;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/created");
    }

    public DispatchPolicy add(String queueName, DispatchPolicy policy) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/add, queueName=" + queueName + ", policy=" + policy);
        policies.put(queueName, policy);
        return policy;
    }

    public DispatchPolicy remove(String queueName) {
        DispatchPolicy dp = policies.remove(queueName);
        if (dp != null)
            dp.close();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/remove, queueName=" + queueName + ", policy=" + dp);
        return dp;
    }

    public DispatchPolicy get(String queueName) {
        DispatchPolicy dp = policies.get(queueName);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/get, queueName=" + queueName + ", policy=" + dp);
        return dp;
    }

    public void removeRouterMetrics(String routerName) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/removeRouterMetrics, routerName=" + routerName + " ...");
        for (Map.Entry<String, DispatchPolicy> entry : policies.entrySet()) {
            ((DispatchPolicy) ((Map.Entry<?, ?>) entry).getValue()).removeMetric(routerName);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/removeRouterMetrics, routerName=" + routerName + " done.");
    }

    public ClusteredQueueMetricCollection getClusteredQueueMetricCollection() {
        List<ClusteredQueueMetric> list = new ArrayList<>();
        for (Map.Entry<String, DispatchPolicy> entry : policies.entrySet()) {
            list.add(((DispatchPolicy) ((Map.Entry) entry).getValue()).getLocalMetric());
        }
        return new ClusteredQueueMetricCollectionImpl(SwiftletManager.getInstance().getRouterName(), list);
    }

    public String toString() {
        return "DispatchPolicyRegistry";
    }
}
