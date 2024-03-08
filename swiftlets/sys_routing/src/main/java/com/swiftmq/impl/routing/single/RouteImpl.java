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

package com.swiftmq.impl.routing.single;

import com.swiftmq.impl.routing.single.schedule.Scheduler;
import com.swiftmq.swiftlet.routing.Route;

import java.util.concurrent.atomic.AtomicReference;

public class RouteImpl extends Route {
    private String destination = null;
    private String outboundQueueName = null;
    private boolean staticRoute = false;
    private final AtomicReference<Scheduler> scheduler = new AtomicReference<>();

    public RouteImpl(String destination, String outboundQueueName, boolean staticRoute, Scheduler scheduler) {
        this.destination = destination;
        this.outboundQueueName = outboundQueueName;
        this.staticRoute = staticRoute;
        this.scheduler.set(scheduler);
    }

    public String getDestination() {
        return destination;
    }

    public String getOutboundQueueName() {
        return outboundQueueName;
    }

    public boolean isStaticRoute() {
        return staticRoute;
    }

    public void setStaticRoute(boolean staticRoute) {
        this.staticRoute = staticRoute;
    }

    public Scheduler getScheduler() {
        return scheduler.get();
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler.set(scheduler);
    }

    public boolean isActive() {
        Scheduler s = scheduler.get();
        return s != null && s.getNumberConnections() > 0;
    }

    public String toString() {
        return "[RouteImpl, destination=" + destination + ", outboundQueueName=" + outboundQueueName + ", staticRoute=" + staticRoute + ", active=" + isActive() + "]";
    }
}
