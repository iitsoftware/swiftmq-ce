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

import com.swiftmq.impl.routing.single.connection.RoutingConnection;
import com.swiftmq.tools.dump.Dumpable;

public interface Route extends Dumpable {
    public final static int ADD = 0;
    public final static int REMOVE = 1;

    public int getType();

    public void setType(int type);

    public String getVersion();

    public void setVersion(String version);

    public String getDestinationRouter();

    public void setDestinationRouter(String destinationRouter);

    public String getKey();

    public String getLastHop();

    public void addHop(String routerName);

    public boolean hasHop(String routerName);

    public int getHopCount();

    public RoutingConnection getRoutingConnection();

    public void setRoutingConnection(RoutingConnection connection);
}
