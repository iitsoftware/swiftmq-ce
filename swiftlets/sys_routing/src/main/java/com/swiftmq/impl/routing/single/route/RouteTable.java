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
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.EntityRemoveException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RouteTable {
    SwiftletContext ctx = null;
    Map<RoutingConnection, ConnectionEntry> connections = null;
    EntityList dynRoutes = null;

    public RouteTable(SwiftletContext ctx) {
        this.ctx = ctx;
        dynRoutes = (EntityList) ctx.usageList.getEntity("routing-table");
        connections = new ConcurrentHashMap<>();
    }

    private void addUsageEntity(Route route) {
        try {
            Entity destEntity = dynRoutes.getEntity(route.getDestinationRouter());
            if (destEntity == null) {
                destEntity = dynRoutes.createEntity();
                destEntity.setName(route.getDestinationRouter());
                destEntity.createCommands();
                dynRoutes.addEntity(destEntity);
            }
            EntityList routes = (EntityList) destEntity.getEntity("dynamic-routes");
            Entity dr = routes.createEntity();
            dr.setName(route.getKey());
            dr.createCommands();
            routes.addEntity(dr);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void removeUsageEntity(Route route) {
        try {
            Entity destEntity = dynRoutes.getEntity(route.getDestinationRouter());
            if (destEntity != null) {
                EntityList routes = (EntityList) destEntity.getEntity("dynamic-routes");
                Entity re = routes.getEntity(route.getKey());
                if (re != null) {
                    routes.removeEntity(re);
                }
                Map map = routes.getEntities();
                if (map == null || map.isEmpty())
                    dynRoutes.removeEntity(destEntity);
            }
        } catch (EntityRemoveException e) {
            e.printStackTrace();
        }
    }

    public void addRoute(Route route) {
        ConnectionEntry ce = connections.get(route.getRoutingConnection());
        if (ce == null) {
            ce = new ConnectionEntry(route.getRoutingConnection());
            connections.put(route.getRoutingConnection(), ce);
        }
        ce.addRoute(route);
        addUsageEntity(route);
    }

    public void removeRoute(Route route) {
        ConnectionEntry ce = connections.get(route.getRoutingConnection());
        if (ce != null) {
            ce.removeRoute(route);
            if (ce.getNumberRoutes() == 0)
                connections.remove(route.getRoutingConnection());
            removeUsageEntity(route);
        }
    }

    public List<Route> getConnectionRoutes(RoutingConnection routingConnection) {
        ConnectionEntry ce = connections.get(routingConnection);
        if (ce != null) {
            return new ArrayList<>(ce.getRoutes().values());
        }
        return null;
    }

    public List<Route> removeConnectionRoutes(RoutingConnection routingConnection) {
        ConnectionEntry ce = (ConnectionEntry) connections.remove(routingConnection);
        if (ce != null) {
            List<Route> al = new ArrayList<>(ce.getRoutes().values());
            al.forEach(route -> {
                route.setType(Route.REMOVE);
                removeUsageEntity(route);
            });
            return al;
        }
        return null;
    }

    public List<RoutingConnection> getRoutingConnections() {
        if (connections.isEmpty())
            return null;
        return (List<RoutingConnection>) new ArrayList(connections.keySet());
    }

    private static class ConnectionEntry {
        RoutingConnection routingConnection = null;
        Map<String, Route> routes = null;

        public ConnectionEntry(RoutingConnection routingConnection) {
            this.routingConnection = routingConnection;
            routes = new ConcurrentHashMap<>();
        }

        public RoutingConnection getRoutingConnection() {
            return routingConnection;
        }

        public void addRoute(Route route) {
            routes.put(route.getKey(), route);
        }

        public void removeRoute(Route route) {
            routes.remove(route.getKey());
        }

        public Map<String, Route> getRoutes() {
            return routes;
        }

        public int getNumberRoutes() {
            return routes.size();
        }

        public String toString() {
            return "[ConnectionEntry, routingConnection=" + routingConnection + ", routes=" + routes + "]";
        }
    }

}
