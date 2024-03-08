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
import java.util.ArrayList;
import java.util.List;

public class RouteImpl implements Route {
    int type = 0;
    String version = null;
    String key = null;
    String destinationRouter = null;
    List<String> hopList = null;
    String lastHop = null;
    RoutingConnection routingConnection = null;

    public RouteImpl(String version, int type, String destinationRouter) {
        this.version = version;
        this.type = type;
        this.destinationRouter = destinationRouter;
        hopList = new ArrayList<>();
    }

    public RouteImpl() {
        this(null, 0, null);
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getDestinationRouter() {
        return destinationRouter;
    }

    public void setDestinationRouter(String destinationRouter) {
        this.destinationRouter = destinationRouter;
    }

    public String getKey() {
        if (key == null) {
            StringBuilder b = new StringBuilder();
            for (int i = hopList.size() - 1; i >= 0; i--) {
                b.append(hopList.get(i));
                if (i > 0)
                    b.append(";");
            }
            key = b.toString();
        }
        return key;
    }

    public String getLastHop() {
        return lastHop;
    }

    public void addHop(String routerName) {
        key = null;
        lastHop = routerName;
        hopList.add(routerName);
    }

    public boolean hasHop(String routerName) {
        return hopList.stream().anyMatch(routerName::equals);
    }

    public int getHopCount() {
        return hopList.size();
    }

    public RoutingConnection getRoutingConnection() {
        return routingConnection;
    }

    public void setRoutingConnection(RoutingConnection routingConnection) {
        this.routingConnection = routingConnection;
    }

    public int getDumpId() {
        return RouteConverter.ROUTE_V400;
    }

    public void writeContent(DataOutput out) throws IOException {
        out.writeInt(getDumpId());
        out.writeInt(type);
        out.writeUTF(version);
        out.writeUTF(destinationRouter);
        out.writeInt(hopList.size());
        for (String s : hopList) {
            out.writeUTF(s);
        }
    }

    public void readContent(DataInput in) throws IOException {
        type = in.readInt();
        version = in.readUTF();
        destinationRouter = in.readUTF();
        int size = in.readInt();
        hopList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            hopList.add(in.readUTF());
        }
    }

    public String toString() {
        return "[RouteImpl, version=" + version + ", type=" + type + ", destinationRouter=" + destinationRouter + ", key=" + key + ", lastHop=" + lastHop + ", hopList=" + hopList + "]";
    }
}
