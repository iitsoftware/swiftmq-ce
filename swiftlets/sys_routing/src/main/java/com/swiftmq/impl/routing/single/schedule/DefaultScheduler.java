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

import com.swiftmq.impl.routing.single.SwiftletContext;
import com.swiftmq.impl.routing.single.connection.RoutingConnection;
import com.swiftmq.impl.routing.single.route.Route;
import com.swiftmq.impl.routing.single.schedule.po.POCloseObject;
import com.swiftmq.swiftlet.routing.event.RoutingEvent;
import com.swiftmq.tools.collection.ConcurrentList;
import com.swiftmq.tools.concurrent.Semaphore;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DefaultScheduler extends Scheduler {
    List<ConnectionEntry> connections = new ConcurrentList<>(new ArrayList<>());

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    public DefaultScheduler(SwiftletContext ctx, String destinationRouter, String queueName) {
        super(ctx, destinationRouter, queueName);
    }

    protected RoutingConnection getNextConnection() {
        lock.writeLock().lock();
        try {
            RoutingConnection rc = null;
            if (connections.isEmpty()) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/getNextConnection, connections.size() == 0, rc=" + rc);
                return null;
            }
            if (connections.size() == 1) {
                rc = connections.get(0).getRoutingConnection();
                if (rc.isClosed()) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/getNextConnection, connections.size() == 1, rc is closed");
                    connections.clear();
                    rc = null;
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/getNextConnection, connections.size() == 1, rc=" + rc);
            } else {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/getNextConnection, connections.size() == " + connections.size() + " ...");
                List<ConnectionEntry> toRemove = new ArrayList<>();
                for (ConnectionEntry entry : connections) {
                    rc = entry.getRoutingConnection();
                    if (rc.isClosed()) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/getNextConnection, connections.size() == " + connections.size() + ", rc is closed");
                        toRemove.add(entry);
                        rc = null;
                    } else {
                        break;
                    }
                }
                connections.removeAll(toRemove);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/getNextConnection, connections.size() == " + connections.size() + ", rc=" + rc);
            }
            if (rc == null && connections.isEmpty()) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/getNextConnection, connections.size() == 0, rc=" + rc + ", destinationDeactivated");
                ctx.routingSwiftlet.fireRoutingEvent("destinationDeactivated", new RoutingEvent(ctx.routingSwiftlet, destinationRouter));
            }

            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/getNextConnection, rc=" + rc);
            return rc;
        } finally {
            lock.writeLock().unlock();
        }

    }

    private ConnectionEntry getConnectionEntry(RoutingConnection rc) {
        return connections.stream().filter(connection -> connection.getRoutingConnection() == rc).findFirst().orElse(null);
    }

    private void requeue(ConnectionEntry ce) {
        remove(ce);
        insert(ce);
    }

    private void remove(ConnectionEntry ce) {
        lock.writeLock().lock();  // Acquire write lock
        try {
            // Iterate with a standard for loop to ensure we modify the underlying list safely
            for (int i = 0; i < connections.size(); i++) {
                ConnectionEntry entry = connections.get(i);
                if (entry != null && ce.getRoutingConnection() == entry.getRoutingConnection()) {
                    connections.remove(i);  // Remove directly from the list
                    break;  // Exit the loop after removal to avoid ConcurrentModificationException
                }
            }
        } finally {
            lock.writeLock().unlock();  // Always ensure the lock is unlocked
        }
    }

    private void insert(ConnectionEntry ce) {
        int idx = -1;
        for (int i = 0; i < connections.size(); i++) {
            ConnectionEntry entry = connections.get(i);
            if (ce.getMinHopCount() <= entry.getMinHopCount()) {
                idx = i;
                break;
            }
        }
        if (idx != -1)
            connections.add(idx, ce);
        else
            connections.add(ce);
    }

    public void addRoute(Route route) {
        lock.writeLock().lock();
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/addRoute, route=" + route);
            ConnectionEntry ce = getConnectionEntry(route.getRoutingConnection());
            if (ce == null) {
                ce = new ConnectionEntry(route.getRoutingConnection());
                ce.addRoute(route);
                insert(ce);
                connectionAdded(route.getRoutingConnection());
            } else {
                ce.addRoute(route);
                requeue(ce);
            }
            if (connections.size() == 1 && ce.getNumberRoutes() == 1)
                ctx.routingSwiftlet.fireRoutingEvent("destinationActivated", new RoutingEvent(ctx.routingSwiftlet, destinationRouter));
        } finally {
            lock.writeLock().unlock();
        }

    }

    public void removeRoute(Route route) {
        lock.writeLock().lock();
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/removeRoute, route=" + route);
            ConnectionEntry ce = getConnectionEntry(route.getRoutingConnection());
            if (ce != null) {
                ce.removeRoute(route);
                if (ce.getNumberRoutes() == 0) {
                    remove(ce);
                    if (connections.isEmpty())
                        ctx.routingSwiftlet.fireRoutingEvent("destinationDeactivated", new RoutingEvent(ctx.routingSwiftlet, destinationRouter));
                } else {
                    requeue(ce);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }

    }

    private boolean _removeRoutingConnection(RoutingConnection routingConnection) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/_removeRoutingConnection, routingConnection=" + routingConnection);
        ConnectionEntry ce = getConnectionEntry(routingConnection);
        if (ce != null) {
            remove(ce);
            return connections.isEmpty();
        }
        return false;
    }

    public void removeRoutingConnection(RoutingConnection routingConnection) {
        lock.writeLock().lock();
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/removeRoutingConnection, routingConnection=" + routingConnection);
            if (_removeRoutingConnection(routingConnection))
                ctx.routingSwiftlet.fireRoutingEvent("destinationDeactivated", new RoutingEvent(ctx.routingSwiftlet, destinationRouter));
        } finally {
            lock.writeLock().unlock();
        }

    }

    public int getNumberConnections() {
        return connections.size();
    }

    public void close() {
        if (closed.getAndSet(true))
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/close ...");
        for (Iterator<ConnectionEntry> iter = connections.iterator(); iter.hasNext(); ) {
            ConnectionEntry entry = iter.next();
            iter.remove();
            connectionRemoved(entry.getRoutingConnection());
        }
        Semaphore sem = new Semaphore();
        enqueueClose(new POCloseObject(null, sem));
        sem.waitHere();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/close done");
    }

    public String toString() {
        return "[DefaultScheduler " + super.toString() + ", n=" + connections.size() + "]";
    }

    protected static class ConnectionEntry {
        RoutingConnection routingConnection = null;
        List<Route> routes = null;
        Set<String> content = null;
        int minHopCount = Integer.MAX_VALUE;

        public ConnectionEntry(RoutingConnection routingConnection) {
            this.routingConnection = routingConnection;
            routes = new ArrayList<>();
            content = new HashSet<>();
        }

        public RoutingConnection getRoutingConnection() {
            return routingConnection;
        }

        public int getMinHopCount() {
            if (minHopCount == Integer.MAX_VALUE) {
                routes.forEach(route -> minHopCount = Math.min(minHopCount, route.getHopCount()));
            }
            return minHopCount;
        }

        public void addRoute(Route route) {
            if (content.contains(route.getKey())) // Route already defined
                return;
            content.add(route.getKey());
            int idx = -1;
            for (int i = 0; i < routes.size(); i++) {
                Route r = routes.get(i);
                if (route.getHopCount() <= r.getHopCount()) {
                    idx = i;
                    break;
                }
            }
            if (idx != -1)
                routes.add(idx, route);
            else
                routes.add(route);
            minHopCount = Integer.MAX_VALUE;
        }

        public void removeRoute(Route route) {
            for (Iterator<Route> iter = routes.iterator(); iter.hasNext(); ) {
                Route r = iter.next();
                if (r.getKey().equals(route.getKey())) {
                    iter.remove();
                    content.remove(route.getKey());
                    break;
                }
            }
            minHopCount = Integer.MAX_VALUE;
        }

        public int getNumberRoutes() {
            return routes.size();
        }

        public String toString() {
            return "[ConnectionEntry, routingConnection=" + routingConnection + ", minHopCount=" + minHopCount + ", routes=" + routes + "]";
        }
    }
}
