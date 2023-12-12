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

package com.swiftmq.impl.routing.single.route;

import com.swiftmq.impl.routing.single.SwiftletContext;
import com.swiftmq.impl.routing.single.connection.RoutingConnection;
import com.swiftmq.impl.routing.single.connection.event.ActivationListener;
import com.swiftmq.impl.routing.single.manager.event.ConnectionEvent;
import com.swiftmq.impl.routing.single.manager.event.ConnectionListener;
import com.swiftmq.impl.routing.single.route.po.POConnectionActivatedObject;
import com.swiftmq.impl.routing.single.route.po.POConnectionRemoveObject;
import com.swiftmq.impl.routing.single.route.po.POExchangeVisitor;
import com.swiftmq.impl.routing.single.route.po.PORouteObject;
import com.swiftmq.impl.routing.single.schedule.Scheduler;
import com.swiftmq.impl.routing.single.smqpr.SendRouteRequest;
import com.swiftmq.mgmt.*;
import com.swiftmq.tools.pipeline.PipelineQueue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RouteExchanger implements ConnectionListener, POExchangeVisitor, ActivationListener {
    static final String TP_EXCHANGER = "sys$routing.route.exchanger";

    static final String VAL_FILTER_TYPE_INCLUDE_BY_HOP = "include_by_hop";
    static final String VAL_FILTER_TYPE_EXCLUDE_BY_HOP = "exclude_by_hop";
    static final String VAL_FILTER_TYPE_INCLUDE_BY_DEST = "include_by_destination";
    static final String VAL_FILTER_TYPE_EXCLUDE_BY_DEST = "exclude_by_destination";

    SwiftletContext ctx = null;
    PipelineQueue pipelineQueue = null;
    RouteTable routeTable = null;
    RouteConverter routeConverter = null;
    Map<String, RouteFilter> filters = new ConcurrentHashMap<>();
    int hopLimit = -1;

    public RouteExchanger(SwiftletContext ctx) {
        this.ctx = ctx;
        Property prop = ctx.root.getProperty("route-announce-hop-limit");
        hopLimit = (Integer) prop.getValue();
        prop.setPropertyChangeListener(new PropertyChangeListener() {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                hopLimit = ((Integer) newValue).intValue();
            }
        });
        routeTable = new RouteTable(ctx);
        routeConverter = new RouteConverter();
        createFilters((EntityList) ctx.root.getEntity("filters"));
        pipelineQueue = new PipelineQueue(ctx.threadpoolSwiftlet.getPool(TP_EXCHANGER), TP_EXCHANGER, this);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/created");
    }

    public RouteConverter getRouteConverter() {
        return routeConverter;
    }

    private void createFilterEntry(RouteFilter routeFilter, EntityList filterEntry) {
        String[] names = filterEntry.getEntityNames();
        if (names != null) {
            for (String name : names)
                routeFilter.addRouterName(name);
        }

        filterEntry.setEntityAddListener(new EntityChangeAdapter(routeFilter) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                RouteFilter myFilter = (RouteFilter) configObject;
                myFilter.addRouterName(newEntity.getName());
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), RouteExchanger.this + "/onEntityAdd (routers): filter=" + myFilter + ", new entry=" + newEntity.getName());
            }
        });
        filterEntry.setEntityRemoveListener(new EntityChangeAdapter(routeFilter) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                RouteFilter myFilter = (RouteFilter) configObject;
                myFilter.removeRouterName(delEntity.getName());
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), RouteExchanger.this + "/onEntityRemove (routers): filter=" + myFilter + ", del entry=" + delEntity.getName());
            }
        });
    }

    private void createFilter(Entity filterEntity) {
        Property prop = filterEntity.getProperty("type");
        String filterType = (String) prop.getValue();
        RouteFilter routeFilter = null;
        if (filterType != null) {
            switch (filterType) {
                case VAL_FILTER_TYPE_INCLUDE_BY_HOP:
                    routeFilter = new RouteFilter(RouteFilter.INCLUDE_BY_HOP);
                    break;
                case VAL_FILTER_TYPE_EXCLUDE_BY_HOP:
                    routeFilter = new RouteFilter(RouteFilter.EXCLUDE_BY_HOP);
                    break;
                case VAL_FILTER_TYPE_INCLUDE_BY_DEST:
                    routeFilter = new RouteFilter(RouteFilter.INCLUDE_BY_DEST);
                    break;
                case VAL_FILTER_TYPE_EXCLUDE_BY_DEST:
                    routeFilter = new RouteFilter(RouteFilter.EXCLUDE_BY_DEST);
                    break;
            }
            createFilterEntry(routeFilter, (EntityList) filterEntity.getEntity("routers"));
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/creating filter for router: " + filterEntity.getName() + ", filter=" + routeFilter);
            filters.put(filterEntity.getName(), routeFilter);
            }
    }

    private void createFilters(EntityList filterList) {
        String[] filterNames = filterList.getEntityNames();
        if (filterNames != null) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$routing", "creating filters ...");
            for (String dest : filterNames) {
                createFilter(filterList.getEntity(dest));
            }
        }

        filterList.setEntityAddListener(new EntityChangeAdapter(null) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                createFilter(newEntity);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), RouteExchanger.this + "/onEntityAdd (filter): filter=" + newEntity.getName());
            }
        });
        filterList.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                    filters.remove(delEntity.getName());
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), RouteExchanger.this + "/onEntityRemove (filter): filter=" + delEntity.getName());
            }
        });
    }

    public void activated(RoutingConnection routingConnection) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/activated, routingConnection=" + routingConnection + " ...");
        Route route = routeConverter.createRoute(routingConnection.getRouterName(), routingConnection.getProtocolVersion(), Route.ADD);
        try {
            processRoute(routingConnection, route);
            pipelineQueue.enqueue(new POConnectionActivatedObject(routingConnection));
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/activated, routingConnection=" + routingConnection + ", exception enqueueRequest: " + e);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/activated, routingConnection=" + routingConnection + " done");
    }

    public void processRoute(RoutingConnection routingConnection, Route route) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/processRoute, routingConnection=" + routingConnection + ", route=" + route);
        route.addHop(routingConnection.getRouterName());
        route.setRoutingConnection(routingConnection);
        pipelineQueue.enqueue(new PORouteObject(route));
    }

    public void connectionAdded(ConnectionEvent evt) {
        // ensure a scheduler is in place (addRoute is called at the RoutingSwiftlet)
        try {
            ctx.schedulerRegistry.getScheduler(evt.getConnection().getRouterName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void connectionRemoved(ConnectionEvent evt) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/connectionRemoved, evt=" + evt);
        pipelineQueue.enqueue(new POConnectionRemoveObject(evt.getConnection()));
    }

    private void sendRoute(RoutingConnection rc, Route route) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/sendRoute, rc=" + rc + ", route= " + route + " ...");
        if (hopLimit > 0 && route.getHopCount() >= hopLimit) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/sendRoute, rc=" + rc + ", route= " + route + ", route.getHopCount() > hopLimit");
            return;
        }
        boolean sameRC = rc == route.getRoutingConnection();
        boolean hasHop = route.hasHop(rc.getRouterName());
        boolean isFiltered = false;
        RouteFilter filter = filters.get(rc.getRouterName());
        isFiltered = filter != null && !filter.isSendable(route);
        if (!sameRC && !hasHop && !isFiltered) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/sendRoute, rc=" + rc + ", route= " + route + " sending...");
            try {
                rc.enqueueRequest(new SendRouteRequest(routeConverter.convert(route, rc.getProtocolVersion())));
            } catch (Exception e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/sendRoute, rc=" + rc + ", route= " + route + ", exception enqueueRequest: " + e);
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/sendRoute, rc=" + rc + ", route= " + route + " done");
    }

    public void visit(PORouteObject po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/visit, po=" + po + " ...");
        Route route = po.getRoute();
        try {
            Scheduler scheduler = ctx.schedulerRegistry.getScheduler(route.getDestinationRouter());
            if (route.getType() == Route.ADD) {
                routeTable.addRoute(route);
                scheduler.addRoute(route);
            } else {
                routeTable.removeRoute(route);
                scheduler.removeRoute(route);
            }
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/visit, po=" + po + ", exception scheduling: " + e);
        }
        List<RoutingConnection> connections = routeTable.getRoutingConnections();
        if (connections != null) {
            connections.forEach(connection -> sendRoute(connection, route));
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/visit, po=" + po + " done");
    }

    public void visit(POConnectionActivatedObject po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/visit, po=" + po + " ...");
        List<RoutingConnection> connections = routeTable.getRoutingConnections();
        if (connections != null) {
            for (RoutingConnection connection : connections) {
                List<Route> routes = routeTable.getConnectionRoutes(connection);
                if (routes != null) {
                    routes.forEach(value -> sendRoute(po.getRoutingConnection(), value));
                }
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/visit, po=" + po + " done");
    }

    public void visit(POConnectionRemoveObject po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/visit, po=" + po + " ...");
        ctx.schedulerRegistry.removeRoutingConnection(po.getRoutingConnection());
        List<Route> routes = routeTable.removeConnectionRoutes(po.getRoutingConnection());
        if (routes != null) {
            for (Route route : routes) {
                try {
                    Scheduler scheduler = ctx.schedulerRegistry.getScheduler(route.getDestinationRouter());
                    scheduler.removeRoute(route);
                    if (scheduler.getNumberConnections() == 0)
                        ctx.schedulerRegistry.removeScheduler(route.getDestinationRouter());
                } catch (Exception e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/visit, po=" + po + ", exception while removing route from scheduler: " + e);
                }
                List<RoutingConnection> connections = routeTable.getRoutingConnections();
                if (connections != null) {
                    connections.forEach(connection -> sendRoute(connection, route));
                }
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/visit, po=" + po + " done");
    }

    public String toString() {
        return "RouteExchanger";
    }
}
