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

package com.swiftmq.impl.routing.single.route.v400;

import com.swiftmq.impl.routing.single.connection.RoutingConnection;
import com.swiftmq.impl.routing.single.route.Route;
import com.swiftmq.impl.routing.single.route.RouteConverter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class RouteImpl implements Route {
    private final AtomicInteger type = new AtomicInteger(0);
    private final AtomicReference<String> version = new AtomicReference<>();
    private final AtomicReference<String> key = new AtomicReference<>();
    private final AtomicReference<String> destinationRouter = new AtomicReference<>();
    private final List<String> hopList = new CopyOnWriteArrayList<>();
    private final AtomicReference<String> lastHop = new AtomicReference<>();
    private final AtomicReference<RoutingConnection> routingConnection = new AtomicReference<>();

    public RouteImpl(String version, int type, String destinationRouter) {
        this.version.set(version);
        this.type.set(type);
        this.destinationRouter.set(destinationRouter);
    }

    public RouteImpl() {
        this(null, 0, null);
    }

    public int getType() {
        return type.get();
    }

    public void setType(int type) {
        this.type.set(type);
    }

    public String getVersion() {
        return version.get();
    }

    public void setVersion(String version) {
        this.version.set(version);
    }

    public String getDestinationRouter() {
        return destinationRouter.get();
    }

    public void setDestinationRouter(String destinationRouter) {
        this.destinationRouter.set(destinationRouter);
    }

    public String getKey() {
        String localKey = key.get();
        if (localKey == null) {
            StringBuilder b = new StringBuilder();
            for (int i = hopList.size() - 1; i >= 0; i--) {
                b.append(hopList.get(i));
                if (i > 0) {
                    b.append(";");
                }
            }
            localKey = b.toString();
            key.compareAndSet(null, localKey);  // Set only if currently null
        }
        return key.get();
    }

    public String getLastHop() {
        return lastHop.get();
    }

    public void addHop(String routerName) {
        key.set(null);  // Invalidate the cache
        lastHop.set(routerName);
        hopList.add(routerName);
    }

    public boolean hasHop(String routerName) {
        return hopList.contains(routerName);
    }

    public int getHopCount() {
        return hopList.size();
    }

    public RoutingConnection getRoutingConnection() {
        return routingConnection.get();
    }

    public void setRoutingConnection(RoutingConnection routingConnection) {
        this.routingConnection.set(routingConnection);
    }

    public int getDumpId() {
        return RouteConverter.ROUTE_V400;
    }

    public void writeContent(DataOutput out) throws IOException {
        out.writeInt(getDumpId());
        out.writeInt(getType());
        out.writeUTF(getVersion());
        out.writeUTF(getDestinationRouter());
        out.writeInt(hopList.size());
        for (String s : hopList) {
            out.writeUTF(s);
        }
    }

    public void readContent(DataInput in) throws IOException {
        type.set(in.readInt());
        version.set(in.readUTF());
        destinationRouter.set(in.readUTF());
        int size = in.readInt();
        hopList.clear();
        for (int i = 0; i < size; i++) {
            hopList.add(in.readUTF());
        }
    }

    public String toString() {
        return "[RouteImpl, version=" + getVersion() + ", type=" + getType() + ", destinationRouter=" + getDestinationRouter() + ", key=" + key.get() + ", lastHop=" + getLastHop() + ", hopList=" + hopList + "]";
    }
}
