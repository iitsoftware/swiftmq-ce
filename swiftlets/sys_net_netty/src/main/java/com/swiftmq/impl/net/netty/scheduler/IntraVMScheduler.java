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

package com.swiftmq.impl.net.netty.scheduler;

import com.swiftmq.impl.net.netty.SwiftletContext;
import com.swiftmq.net.client.IntraVMConnection;
import com.swiftmq.swiftlet.net.ConnectionVetoException;
import com.swiftmq.swiftlet.net.IntraVMListenerMetaData;
import com.swiftmq.swiftlet.net.event.ConnectionListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IntraVMScheduler {
    SwiftletContext ctx;
    Map<String, IntraVMListenerMetaData> listeners = new ConcurrentHashMap<>();

    public IntraVMScheduler(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    public void createListener(IntraVMListenerMetaData metaData)
            throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$net", toString() + "/createListener: MetaData=" + metaData);
        listeners.put(metaData.getSwiftlet().getName(), metaData);
    }

    public void removeListener(IntraVMListenerMetaData metaData) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$net", toString() + "/removeListener: metaData=" + metaData);
        listeners.remove(metaData.getSwiftlet().getName());
    }

    public void connectListener(String swiftletName, IntraVMConnection clientConnection)
            throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$net", toString() + "/connectListener: swiftletName=" + swiftletName + ", clientConnection=" + clientConnection);
        IntraVMListenerMetaData metaData = (IntraVMListenerMetaData) listeners.get(swiftletName);
        if (metaData == null)
            throw new Exception("No intra-VM listener found for Swiftlet '" + swiftletName + "'");
        IntraVMServerEndpointImpl connection = new IntraVMServerEndpointImpl(ctx, clientConnection);
        try {
            ConnectionListener connectionListener = metaData.getConnectionListener();
            connection.setConnectionListener(connectionListener);
            connection.setMetaData(metaData);
            connectionListener.connected(connection);
            ctx.networkSwiftlet.getConnectionManager().addConnection(connection);
        } catch (ConnectionVetoException cve) {
            connection.close();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$net", toString() + "/ConnectionVetoException: " + cve.getMessage());
            ctx.logSwiftlet.logError("sys$net", toString() + "/ConnectionVetoException: " + cve.getMessage());
            throw new Exception(cve.getMessage());
        }

        ctx.logSwiftlet.logInformation("sys$net", toString() + "/connection accepted: " + connection);
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$net", toString() + "/close");
        listeners.clear();
    }

    public String toString() {
        return "IntraVMScheduler";
    }
}
