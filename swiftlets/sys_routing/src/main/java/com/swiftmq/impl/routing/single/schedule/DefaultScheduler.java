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
import com.swiftmq.tools.concurrent.Semaphore;

import java.util.*;

public class DefaultScheduler extends Scheduler {
    List connections = new ArrayList();

    public DefaultScheduler(SwiftletContext ctx, String destinationRouter, String queueName) {
        super(ctx, destinationRouter, queueName);
    }

    protected synchronized RoutingConnection getNextConnection() {
        RoutingConnection rc = null;
        if (connections.size() == 0) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/getNextConnection, connections.size() == 0, rc=" + rc);
            return null;
        }
        if (connections.size() == 1) {
            rc = ((ConnectionEntry) connections.get(0)).getRoutingConnection();
            if (rc.isClosed()) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/getNextConnection, connections.size() == 1, rc is closed");
                connections.clear();
                rc = null;
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/getNextConnection, connections.size() == 1, rc=" + rc);
        } else {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/getNextConnection, connections.size() == " + connections.size() + " ...");
            for (Iterator iter = connections.iterator(); iter.hasNext(); ) {
                rc = ((ConnectionEntry) iter.next()).getRoutingConnection();
                if (rc.isClosed()) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/getNextConnection, connections.size() == " + connections.size() + ", rc is closed");
                    iter.remove();
                    rc = null;
                } else
                    break;
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/getNextConnection, connections.size() == " + connections.size() + ", rc=" + rc);
        }
        if (rc == null && connections.size() == 0) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/getNextConnection, connections.size() == 0, rc=" + rc + ", destinationDeactivated");
            ctx.routingSwiftlet.fireRoutingEvent("destinationDeactivated", new RoutingEvent(ctx.routingSwiftlet, destinationRouter));
        }

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/getNextConnection, rc=" + rc);
        return rc;
    }

    private ConnectionEntry getConnectionEntry(RoutingConnection rc) {
        for (int i = 0; i < connections.size(); i++) {
            ConnectionEntry ce = (ConnectionEntry) connections.get(i);
            if (ce.getRoutingConnection() == rc)
                return ce;
        }
        return null;
    }

    private void requeue(ConnectionEntry ce) {
        remove(ce);
        insert(ce);
    }

    private void remove(ConnectionEntry ce) {
        for (Iterator iter = connections.iterator(); iter.hasNext(); ) {
            ConnectionEntry entry = (ConnectionEntry) iter.next();
            if (ce.getRoutingConnection() == entry.getRoutingConnection()) {
                iter.remove();
                break;
            }
        }
    }

    private void insert(ConnectionEntry ce) {
        int idx = -1;
        for (int i = 0; i < connections.size(); i++) {
            ConnectionEntry entry = (ConnectionEntry) connections.get(i);
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

    public synchronized void addRoute(Route route) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/addRoute, route=" + route);
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
    }

    public synchronized void removeRoute(Route route) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/removeRoute, route=" + route);
        ConnectionEntry ce = getConnectionEntry(route.getRoutingConnection());
        if (ce != null) {
            ce.removeRoute(route);
            if (ce.getNumberRoutes() == 0) {
                remove(ce);
                if (connections.size() == 0)
                    ctx.routingSwiftlet.fireRoutingEvent("destinationDeactivated", new RoutingEvent(ctx.routingSwiftlet, destinationRouter));
            } else {
                requeue(ce);
            }
        }
    }

    private synchronized boolean _removeRoutingConnection(RoutingConnection routingConnection) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/_removeRoutingConnection, routingConnection=" + routingConnection);
        ConnectionEntry ce = getConnectionEntry(routingConnection);
        if (ce != null) {
            remove(ce);
            return connections.size() == 0;
        }
        return false;
    }

    public void removeRoutingConnection(RoutingConnection routingConnection) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/removeRoutingConnection, routingConnection=" + routingConnection);
        if (_removeRoutingConnection(routingConnection))
            ctx.routingSwiftlet.fireRoutingEvent("destinationDeactivated", new RoutingEvent(ctx.routingSwiftlet, destinationRouter));
    }

    public synchronized int getNumberConnections() {
        return connections.size();
    }

    public void close() {
//debug    ctx.timerSwiftlet.removeTimerListener(tl);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/close ...");
        synchronized (this) {
            if (closed)
                return;
            for (Iterator iter = connections.iterator(); iter.hasNext(); ) {
                ConnectionEntry entry = (ConnectionEntry) iter.next();
                iter.remove();
                connectionRemoved(entry.getRoutingConnection());
            }
        }
        Semaphore sem = new Semaphore();
        enqueueClose(new POCloseObject(null, sem));
        sem.waitHere();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/close done");
    }

    public String toString() {
        return "[DefaultScheduler " + super.toString() + ", n=" + connections.size() + "]";
    }

    protected class ConnectionEntry {
        RoutingConnection routingConnection = null;
        List routes = null;
        Set content = null;
        int minHopCount = Integer.MAX_VALUE;

        public ConnectionEntry(RoutingConnection routingConnection) {
            this.routingConnection = routingConnection;
            routes = new ArrayList();
            content = new HashSet();
        }

        public RoutingConnection getRoutingConnection() {
            return routingConnection;
        }

        public int getMinHopCount() {
            if (minHopCount == Integer.MAX_VALUE) {
                for (int i = 0; i < routes.size(); i++) {
                    Route r = (Route) routes.get(i);
                    minHopCount = Math.min(minHopCount, r.getHopCount());
                }
            }
            return minHopCount;
        }

        public void addRoute(Route route) {
            if (content.contains(route.getKey())) // Route already defined
            {
                return;
            }
            content.add(route.getKey());
            int idx = -1;
            for (int i = 0; i < routes.size(); i++) {
                Route r = (Route) routes.get(i);
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
            for (Iterator iter = routes.iterator(); iter.hasNext(); ) {
                Route r = (Route) iter.next();
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
