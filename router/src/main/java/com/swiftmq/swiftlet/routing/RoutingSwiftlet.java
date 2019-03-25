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

package com.swiftmq.swiftlet.routing;

import com.swiftmq.swiftlet.Swiftlet;
import com.swiftmq.swiftlet.routing.event.RoutingEvent;
import com.swiftmq.swiftlet.routing.event.RoutingListener;

import java.util.*;

/**
 * The RoutingSwiftlet manages connections as well as message routing
 * to remote destinations.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public abstract class RoutingSwiftlet extends Swiftlet {
    Map routingTable = new HashMap();
    ArrayList listeners = new ArrayList();


    /**
     * Returns a route for a remote router.
     *
     * @param destination router name.
     * @return route.
     */
    public Route getRoute(String destination) {
        Route route = null;
        synchronized (routingTable) {
            route = (Route) routingTable.get(destination);
        }
        return route;
    }


    /**
     * Returns all available routes.
     *
     * @return routes.
     */
    public Route[] getRoutes() {
        Route[] routes = null;
        synchronized (routingTable) {
            if (routingTable.size() > 0) {
                routes = new Route[routingTable.size()];
                Set set = routingTable.entrySet();
                Iterator iter = set.iterator();
                int i = 0;
                while (iter.hasNext())
                    routes[i++] = (Route) ((Map.Entry) iter.next()).getValue();
            }
        }
        return routes;
    }


    /**
     * Adds a route.
     *
     * @param route route.
     */
    protected void addRoute(Route route) {
        synchronized (routingTable) {
            routingTable.put(route.getDestination(), route);
        }
        fireRoutingEvent("destinationAdded", new RoutingEvent(this, route.getDestination()));
    }


    /**
     * Removes a route.
     *
     * @param route route.
     */
    protected void removeRoute(Route route) {
        synchronized (routingTable) {
            routingTable.remove(route.getDestination());
        }
        fireRoutingEvent("destinationRemoved", new RoutingEvent(this, route.getDestination()));
    }


    /**
     * Removes all routes.
     */
    protected void removeAllRoutes() {
        Route[] routes = getRoutes();
        if (routes != null) {
            for (int i = 0; i < routes.length; i++)
                removeRoute(routes[i]);
        }
    }


    /**
     * Adds a routing listener.
     *
     * @param l listener.
     */
    public void addRoutingListener(RoutingListener l) {
        synchronized (listeners) {
            listeners.add(l);
        }
    }

    /**
     * Removes a routing listener.
     *
     * @param l listener.
     */
    public void removeRoutingListener(RoutingListener l) {
        synchronized (listeners) {
            listeners.remove(l);
        }
    }

    /**
     * Removes all routing listeners.
     */
    protected void removeAllRoutingListeners() {
        synchronized (listeners) {
            listeners.clear();
        }
    }


    /**
     * Fires a routing event.
     * Internal use only.
     *
     * @param method method to call.
     * @param evt    event.
     */
    public void fireRoutingEvent(String method, RoutingEvent evt) {
        synchronized (listeners) {
            for (int i = 0; i < listeners.size(); i++) {
                RoutingListener l = (RoutingListener) listeners.get(i);
                if (method.equals("destinationAdded")) {
                    l.destinationAdded(evt);
                } else if (method.equals("destinationRemoved")) {
                    l.destinationRemoved(evt);
                } else if (method.equals("destinationActivated")) {
                    l.destinationActivated(evt);
                } else if (method.equals("destinationDeactivated")) {
                    l.destinationDeactivated(evt);
                }
            }
        }
    }
}

