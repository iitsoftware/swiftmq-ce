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

package com.swiftmq.impl.routing.single.manager;

import com.swiftmq.impl.routing.single.SwiftletContext;
import com.swiftmq.impl.routing.single.connection.RoutingConnection;
import com.swiftmq.impl.routing.single.manager.event.ConnectionEvent;
import com.swiftmq.impl.routing.single.manager.event.ConnectionListener;
import com.swiftmq.impl.routing.single.manager.po.POAddObject;
import com.swiftmq.impl.routing.single.manager.po.POCMVisitor;
import com.swiftmq.impl.routing.single.manager.po.PORemoveAllObject;
import com.swiftmq.impl.routing.single.manager.po.PORemoveObject;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.Property;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.PipelineQueue;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConnectionManager
        implements POCMVisitor {
    static final String TP_CONNMGR = "sys$routing.connection.mgr";

    PipelineQueue queue = null;
    SwiftletContext ctx = null;
    protected Map<String, RoutingConnection> connections = new HashMap<>();
    List<ConnectionListener> listeners = new CopyOnWriteArrayList<>();
    EntityList connectionEntity = null;
    final AtomicBoolean closed = new AtomicBoolean(false);

    public ConnectionManager(SwiftletContext ctx) {
        this.ctx = ctx;
        connectionEntity = (EntityList) ctx.usageList.getEntity("connections");
        queue = new PipelineQueue(ctx.threadpoolSwiftlet.getPool(TP_CONNMGR), TP_CONNMGR, this);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "ConnectionManager/created");
    }

    protected boolean isLicenseLimit() {
        return !connections.isEmpty();
    }

    public void visit(POAddObject poa) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "ConnectionManager/visitConnectionAdd...");

        RoutingConnection rc = poa.getConnection();
        String exception = null;
        if (isLicenseLimit())
            exception = "License limit reached. Connection REJECTED!";
        else if (rc.getRouterName().equals(ctx.routerName) || connections.containsKey(rc.getRouterName()))
            exception = "Router '" + rc.getRouterName() + "' is already connected. Connection REJECTED!";
        if (exception != null) {
            poa.setSuccess(false);
            poa.setException(exception);
            if (poa.getCallback() != null)
                poa.getCallback().onException(poa);
        } else {
            connections.put(rc.getRouterName(), rc);
            try {
                Entity ce = connectionEntity.createEntity();
                ce.setName(rc.getConnectionId());
                ce.setDynamicObject(rc);
                ce.createCommands();
                Property prop = ce.getProperty("routername");
                prop.setReadOnly(false);
                prop.setValue(rc.getRouterName());
                prop.setReadOnly(true);
                prop = ce.getProperty("connecttime");
                prop.setValue(new Date().toString());
                prop.setReadOnly(true);
                connectionEntity.addEntity(ce);
                rc.setUsageEntity(ce);
            } catch (Exception e) {
            }
            rc.setActivationListener(ctx.routeExchanger);
            poa.setSuccess(true);
            if (poa.getCallback() != null)
                poa.getCallback().onSuccess(poa);
        }

        if (poa.getSemaphore() != null)
            poa.getSemaphore().notifySingleWaiter();

        if (poa.isSuccess())
            fireConnectionAdded(new ConnectionEvent(rc));

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "ConnectionManager/visitConnectionAdd done");
    }

    private void removeConnection(RoutingConnection rc, boolean mapRemove) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "ConnectionManager/removeConnection...");

        connectionEntity.removeDynamicEntity(rc);
        if (mapRemove)
            connections.remove(rc.getRouterName());
        rc.close();

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "ConnectionManager/removeConnection done");
    }

    private void removeAll() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "ConnectionManager/removeAll...");

        for (Iterator<Map.Entry<String, RoutingConnection>> iter = connections.entrySet().iterator(); iter.hasNext(); ) {
            RoutingConnection rc = iter.next().getValue();
            removeConnection(rc, false);
            iter.remove();
            fireConnectionRemoved(new ConnectionEvent(rc));
        }

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "ConnectionManager/removeAll done");
    }

    public void visit(PORemoveObject por) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "ConnectionManager/visitConnectionRemove...");

        RoutingConnection rc = por.getConnection();
        removeConnection(rc, true);
        por.setSuccess(true);
        if (por.getCallback() != null)
            por.getCallback().onSuccess(por);
        if (por.getSemaphore() != null)
            por.getSemaphore().notifySingleWaiter();

        fireConnectionRemoved(new ConnectionEvent(rc));

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "ConnectionManager/visitConnectionRemove done");
    }

    public void visit(PORemoveAllObject po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "ConnectionManager/visitConnectionRemoveAll...");

        removeAll();
        po.setSuccess(true);
        if (po.getCallback() != null)
            po.getCallback().onSuccess(po);
        if (po.getSemaphore() != null)
            po.getSemaphore().notifySingleWaiter();

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "ConnectionManager/visitConnectionRemoveAll done");
    }

    public void addConnectionListener(ConnectionListener l) {
        listeners.add(l);
    }

    public void removeConnectionListener(ConnectionListener l) {
            listeners.remove(l);
    }

    private void fireConnectionAdded(ConnectionEvent evt) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "ConnectionManager/fireConnectionAdded...");
        listeners.forEach(listener -> listener.connectionAdded(evt));
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "ConnectionManager/fireConnectionAdded done");
    }

    private void fireConnectionRemoved(ConnectionEvent evt) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "ConnectionManager/fireConnectionRemoved...");
        listeners.forEach(listener -> listener.connectionRemoved(evt));
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "ConnectionManager/fireConnectionRemoved done");
    }

    public void enqueue(POObject obj) {
        queue.enqueue(obj);
    }

    public void close() {
        if (closed.getAndSet(true))
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "ConnectionManager/close...");
        queue.close();
        connections.clear();
        listeners.clear();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "ConnectionManager/close done");
    }
}
