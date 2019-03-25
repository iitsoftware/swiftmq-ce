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

package com.swiftmq.impl.net.standard.scheduler;

import com.swiftmq.net.client.IntraVMConnection;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.net.ConnectionManager;
import com.swiftmq.swiftlet.net.ConnectionVetoException;
import com.swiftmq.swiftlet.net.IntraVMListenerMetaData;
import com.swiftmq.swiftlet.net.NetworkSwiftlet;
import com.swiftmq.swiftlet.net.event.ConnectionListener;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;

import java.util.HashMap;
import java.util.Map;

public class IntraVMScheduler {
    NetworkSwiftlet networkSwiftlet = null;
    LogSwiftlet logSwiftlet = null;
    TraceSwiftlet traceSwiftlet = null;
    TraceSpace traceSpace = null;
    ConnectionManager connectionManager = null;
    Map listeners = new HashMap();

    public IntraVMScheduler(NetworkSwiftlet networkSwiftlet) {
        this.networkSwiftlet = networkSwiftlet;
        logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
        connectionManager = networkSwiftlet.getConnectionManager();
    }

    public synchronized void createListener(IntraVMListenerMetaData metaData)
            throws Exception {
        if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/createListener: MetaData=" + metaData);
        listeners.put(metaData.getSwiftlet().getName(), metaData);
    }

    public synchronized void removeListener(IntraVMListenerMetaData metaData) {
        if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/removeListener: metaData=" + metaData);
        listeners.remove(metaData.getSwiftlet().getName());
    }

    public synchronized void connectListener(String swiftletName, IntraVMConnection clientConnection)
            throws Exception {
        if (traceSpace.enabled)
            traceSpace.trace("sys$net", toString() + "/connectListener: swiftletName=" + swiftletName + ", clientConnection=" + clientConnection);
        IntraVMListenerMetaData metaData = (IntraVMListenerMetaData) listeners.get(swiftletName);
        if (metaData == null)
            throw new Exception("No intra-VM listener found for Swiftlet '" + swiftletName + "'");
        IntraVMServerEndpointImpl connection = new IntraVMServerEndpointImpl(clientConnection);
        try {
            ConnectionListener connectionListener = metaData.getConnectionListener();
            connection.setConnectionListener(connectionListener);
            connection.setMetaData(metaData);
            connectionListener.connected(connection);
            connectionManager.addConnection(connection);
        } catch (ConnectionVetoException cve) {
            connection.close();
            if (traceSpace.enabled)
                traceSpace.trace("sys$net", toString() + "/ConnectionVetoException: " + cve.getMessage());
            logSwiftlet.logError("sys$net", toString() + "/ConnectionVetoException: " + cve.getMessage());
            throw new Exception(cve.getMessage());
        }

        logSwiftlet.logInformation("sys$net", toString() + "/connection accepted: " + connection);
    }

    public synchronized void close() {
        if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/close");
        listeners.clear();
    }

    public String toString() {
        return "IntraVMScheduler";
    }
}
