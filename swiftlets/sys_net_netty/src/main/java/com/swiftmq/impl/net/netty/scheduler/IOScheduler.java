
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
import com.swiftmq.net.SocketFactory;
import com.swiftmq.swiftlet.net.ConnectorMetaData;
import com.swiftmq.swiftlet.net.ListenerMetaData;
import com.swiftmq.tools.collection.ArrayListTool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public abstract class IOScheduler {
    SwiftletContext ctx;
    ArrayList listeners = new ArrayList();
    ArrayList connectors = new ArrayList();
    Map socketFactories = new HashMap();

    public IOScheduler(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    private SocketFactory getSocketFactory(String className)
            throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$net", toString() + "/getSocketFactory: className=" + className);
        SocketFactory sf = (SocketFactory) socketFactories.get(className);
        if (sf == null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$net", toString() + "/getSocketFactory: className=" + className + ", creating...");
            sf = (SocketFactory) Class.forName(className).newInstance();
            socketFactories.put(className, sf);
        }
        return sf;
    }

    public synchronized int createListener(ListenerMetaData metaData)
            throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$net", toString() + "/createListener: MetaData=" + metaData);
        TCPListener l = createListenerInstance(metaData, getSocketFactory(metaData.getSocketFactoryClass()));
        return ArrayListTool.setFirstFreeOrExpand(listeners, l);
    }

    public synchronized TCPListener getListener(int listenerId) {
        return (TCPListener) listeners.get(listenerId);
    }

    protected abstract TCPListener createListenerInstance(ListenerMetaData metaData, SocketFactory socketFactory)
            throws Exception;

    public synchronized void removeListener(int listenerId) {
        TCPListener l = (TCPListener) listeners.get(listenerId);
        if (l != null) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$net", toString() + "/removeListener: id=" + listenerId);
            listeners.set(listenerId, null);
            l.close();
        }
    }

    public synchronized int createConnector(ConnectorMetaData metaData)
            throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$net", toString() + "/createConnector: MetaData=" + metaData);
        TCPConnector c = createConnectorInstance(metaData, getSocketFactory(metaData.getSocketFactoryClass()));
        return ArrayListTool.setFirstFreeOrExpand(connectors, c);
    }

    public synchronized TCPConnector getConnector(int connectorId) {
        return (TCPConnector) connectors.get(connectorId);
    }

    protected abstract TCPConnector createConnectorInstance(ConnectorMetaData metaData, SocketFactory socketFactory)
            throws Exception;

    public synchronized void removeConnector(int connectorId) {
        TCPConnector c = (TCPConnector) connectors.get(connectorId);
        if (c != null) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$net", toString() + "/removeConnector: id=" + connectorId);
            connectors.set(connectorId, null);
            c.close();
        }
    }

    public synchronized void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$net", toString() + "/close: listeners");
        for (int i = 0; i < listeners.size(); i++) {
            TCPListener l = (TCPListener) listeners.get(i);
            if (l != null)
                l.close();
        }
        listeners.clear();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$net", toString() + "/close: connectore");
        for (int i = 0; i < connectors.size(); i++) {
            TCPConnector c = (TCPConnector) connectors.get(i);
            if (c != null)
                c.close();
        }
        connectors.clear();
        socketFactories.clear();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$net", toString() + "/close: done.");
    }
}

