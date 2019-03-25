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

package com.swiftmq.impl.routing.single.manager.po;

import com.swiftmq.impl.routing.single.connection.RoutingConnection;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POCallback;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.POVisitor;

public class POAddObject extends POObject {
    RoutingConnection connection = null;

    public POAddObject(POCallback callback, Semaphore semaphore, RoutingConnection connection) {
        super(callback, semaphore);
        this.connection = connection;
    }

    public void accept(POVisitor visitor) {
        ((POCMVisitor) visitor).visit(this);
    }

    public RoutingConnection getConnection() {
        return connection;
    }

    public String toString() {
        return "[POAddObject, connection=" + connection + "]";
    }
}
