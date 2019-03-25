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

package com.swiftmq.impl.routing.single.smqpr.v400;

import com.swiftmq.impl.routing.single.route.Route;
import com.swiftmq.impl.routing.single.smqpr.SMQRVisitor;
import com.swiftmq.tools.dump.DumpableFactory;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RouteRequest extends Request {
    Route route = null;
    byte[] buffer = null;

    public RouteRequest(Route route) {
        super(0, false);
        this.route = route;
    }

    public RouteRequest() {
        this(null);
    }

    public Route getRoute(DumpableFactory factory) throws Exception {
        DataByteArrayInputStream dbis = new DataByteArrayInputStream(buffer);
        route = (Route) factory.createDumpable(dbis.readInt());
        if (route == null)
            throw new Exception("Unable to create Route!");
        route.readContent(dbis);
        return route;
    }

    protected Reply createReplyInstance() {
        return null;
    }

    public void accept(RequestVisitor visitor) {
        ((SMQRVisitor) visitor).handleRequest(this);
    }

    public int getDumpId() {
        return SMQRFactory.ROUTE_REQ;
    }

    public void writeContent(DataOutput output) throws IOException {
        super.writeContent(output);
        DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();
        route.writeContent(dbos);
        output.writeInt(dbos.getCount());
        output.write(dbos.getBuffer(), 0, dbos.getCount());
    }

    public void readContent(DataInput input) throws IOException {
        super.readContent(input);
        buffer = new byte[input.readInt()];
        input.readFully(buffer);
    }

    public String toString() {
        return "[RouteRequest " + super.toString() + ", route=" + route + ", buffer=" + buffer + "]";
    }
}
