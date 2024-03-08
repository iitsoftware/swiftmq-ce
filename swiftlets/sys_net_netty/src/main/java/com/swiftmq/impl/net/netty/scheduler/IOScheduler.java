
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
import com.swiftmq.tools.collection.ConcurrentExpandableList;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

public abstract class IOScheduler {
    SwiftletContext ctx;
    ConcurrentExpandableList listeners = new ConcurrentExpandableList();
    ConcurrentExpandableList connectors = new ConcurrentExpandableList();
    Map<String, SocketFactory> socketFactories = new ConcurrentHashMap<>();

    public IOScheduler(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    private SocketFactory getSocketFactory(String className)
            throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$net", toString() + "/getSocketFactory: className=" + className);
        SocketFactory sf = socketFactories.get(className);
        if (sf == null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$net", toString() + "/getSocketFactory: className=" + className + ", creating...");
            sf = (SocketFactory) Class.forName(className).newInstance();
            socketFactories.put(className, sf);
        }
        return sf;
    }

    public int createListener(ListenerMetaData metaData)
            throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$net", toString() + "/createListener: MetaData=" + metaData);
        TCPListener l = createListenerInstance(metaData, getSocketFactory(metaData.getSocketFactoryClass()));
        return listeners.add(l);
    }

    public TCPListener getListener(int listenerId) {
        return (TCPListener) listeners.get(listenerId);
    }

    protected abstract TCPListener createListenerInstance(ListenerMetaData metaData, SocketFactory socketFactory)
            throws Exception;

    public void removeListener(int listenerId) {
        TCPListener l = (TCPListener) listeners.get(listenerId);
        if (l != null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$net", toString() + "/removeListener: id=" + listenerId);
            listeners.remove(listenerId);
            l.close();
        }
    }

    public int createConnector(ConnectorMetaData metaData)
            throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$net", toString() + "/createConnector: MetaData=" + metaData);
        TCPConnector c = createConnectorInstance(metaData, getSocketFactory(metaData.getSocketFactoryClass()));
        return connectors.add(c);
    }

    public TCPConnector getConnector(int connectorId) {
        return (TCPConnector) connectors.get(connectorId);
    }

    protected abstract TCPConnector createConnectorInstance(ConnectorMetaData metaData, SocketFactory socketFactory)
            throws Exception;

    public void removeConnector(int connectorId) {
        TCPConnector c = (TCPConnector) connectors.get(connectorId);
        if (c != null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$net", toString() + "/removeConnector: id=" + connectorId);
            connectors.remove(connectorId);
            c.close();
        }
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$net", toString() + "/close: listeners");
        IntStream.range(0, listeners.size()).mapToObj(i -> (TCPListener) listeners.get(i)).filter(Objects::nonNull).forEach(TCPListener::close);
        listeners.clear();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$net", toString() + "/close: connectore");
        IntStream.range(0, connectors.size()).mapToObj(i -> (TCPConnector) connectors.get(i)).filter(Objects::nonNull).forEach(TCPConnector::close);
        connectors.clear();
        socketFactories.clear();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$net", toString() + "/close: done.");
    }
}

