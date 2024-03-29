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

package com.swiftmq.impl.routing.single.schedule;

import com.swiftmq.impl.routing.single.RouteImpl;
import com.swiftmq.impl.routing.single.SwiftletContext;
import com.swiftmq.impl.routing.single.connection.RoutingConnection;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.UnknownQueueException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SchedulerRegistry {
    // Outbound Routing-Queue Prefix
    public static final String QUEUE_PREFIX = "rt$";

    // Outbound Redirection Predicate
    static final String OUTBOUND_REDIR_PRED = "%@";

    SwiftletContext ctx = null;
    Map<String, Scheduler> schedulers = new HashMap<>();
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public SchedulerRegistry(SwiftletContext ctx) {
        this.ctx = ctx;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/created");
    }

    public Scheduler getScheduler(String destinationRouter) throws Exception {
        lock.writeLock().lock();
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/getScheduler, destinationRouter=" + destinationRouter);
            Scheduler scheduler = (Scheduler) schedulers.get(destinationRouter);
            if (scheduler == null) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/getScheduler, destinationRouter=" + destinationRouter + ", creating scheduler...");
                String queueName = QUEUE_PREFIX + destinationRouter + "@" + ctx.routerName;
                if (!ctx.queueManager.isQueueDefined(queueName))
                    ctx.queueManager.createQueue(queueName, (ActiveLogin) null);
                ctx.queueManager.setQueueOutboundRedirector(OUTBOUND_REDIR_PRED + destinationRouter, queueName);
                if (ctx.roundRobinEnabled) {
                    scheduler = new RoundRobinScheduler(ctx, destinationRouter, queueName);
                } else {
                    scheduler = new DefaultScheduler(ctx, destinationRouter, queueName);
                }
                schedulers.put(destinationRouter, scheduler);
                RouteImpl route = (RouteImpl) ctx.routingSwiftlet.getRoute(destinationRouter);
                if (route == null)
                    ctx.routingSwiftlet.addRoute(new RouteImpl(destinationRouter, queueName, true, scheduler));
                else
                    route.setScheduler(scheduler);
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/getScheduler, destinationRouter=" + destinationRouter + ", returns " + scheduler);
            return scheduler;
        } finally {
            lock.writeLock().unlock();
        }

    }

    public void removeScheduler(String destinationRouter) throws Exception {
        lock.writeLock().lock();
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/removeScheduler, destinationRouter=" + destinationRouter);
            Scheduler scheduler = (Scheduler) schedulers.remove(destinationRouter);
            if (scheduler != null) {
                scheduler.close();
                RouteImpl route = (RouteImpl) ctx.routingSwiftlet.getRoute(destinationRouter);
                if (route != null) {
                    if (route.isStaticRoute())
                        route.setScheduler(null);
                    else {
                        ctx.queueManager.setQueueOutboundRedirector(OUTBOUND_REDIR_PRED + destinationRouter, null);
                        ctx.routingSwiftlet.removeRoute(route);
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }

    }

    public void removeRoutingConnection(RoutingConnection routingConnection) {
        lock.writeLock().lock();
        try {
            for (Iterator iter = schedulers.entrySet().iterator(); iter.hasNext(); ) {
                Scheduler scheduler = (Scheduler) ((Map.Entry) iter.next()).getValue();
                scheduler.removeRoutingConnection(routingConnection);
                if (scheduler.getNumberConnections() == 0) {
                    scheduler.close();
                    iter.remove();
                }
            }
        } finally {
            lock.writeLock().unlock();
        }

    }

    public void close() {
        lock.writeLock().lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/close ...");
            for (Map.Entry<String, Scheduler> entry : schedulers.entrySet()) {
                Scheduler scheduler = entry.getValue();
                try {
                    ctx.queueManager.setQueueOutboundRedirector(OUTBOUND_REDIR_PRED + scheduler.getQueueName(), null);
                } catch (UnknownQueueException e) {
                }
                scheduler.close();
            }
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/close done");
        } finally {
            lock.writeLock().unlock();
        }

    }

    public String toString() {
        return "SchedulerRegistry";
    }
}
