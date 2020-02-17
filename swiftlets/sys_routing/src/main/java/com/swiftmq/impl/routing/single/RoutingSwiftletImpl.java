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

package com.swiftmq.impl.routing.single;

import com.swiftmq.impl.routing.single.accounting.AccountingProfile;
import com.swiftmq.impl.routing.single.accounting.RoutingSourceFactory;
import com.swiftmq.impl.routing.single.connection.RoutingConnection;
import com.swiftmq.impl.routing.single.jobs.JobRegistrar;
import com.swiftmq.impl.routing.single.manager.po.PORemoveAllObject;
import com.swiftmq.impl.routing.single.manager.po.PORemoveObject;
import com.swiftmq.impl.routing.single.schedule.Scheduler;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.event.SwiftletManagerAdapter;
import com.swiftmq.swiftlet.event.SwiftletManagerEvent;
import com.swiftmq.swiftlet.net.*;
import com.swiftmq.swiftlet.routing.RoutingSwiftlet;
import com.swiftmq.swiftlet.scheduler.SchedulerSwiftlet;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.util.SwiftUtilities;

import java.net.InetAddress;
import java.util.*;

public class RoutingSwiftletImpl extends RoutingSwiftlet {
    public static final String UNROUTABLE_QUEUE = "unroutable";

    protected Configuration config = null;
    protected Entity root = null;
    protected SwiftletContext ctx = null;
    Map passwords = null;
    Map connectionEntities = null;
    Set connections = null;
    Acceptor acceptor = null;
    Semaphore shutdownSem = null;
    JobRegistrar jobRegistrar = null;
    RoutingSourceFactory sourceFactory = null;
    AccountingProfile accountingProfile = null;

    public synchronized AccountingProfile getAccountingProfile() {
        return accountingProfile;
    }

    public void setAccountingProfile(AccountingProfile accountingProfile) {
        synchronized (this) {
            this.accountingProfile = accountingProfile;
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "setAccountingProfile, accountingProfile= " + accountingProfile);
        Connection[] c = (Connection[]) connections.toArray(new Connection[connections.size()]);
        for (int i = 0; i < c.length; i++) {
            RoutingConnection rc = (RoutingConnection) c[i].getUserObject();
            if (rc != null) {
                if (accountingProfile != null)
                    rc.startAccounting(accountingProfile);
                else
                    rc.stopAccounting();
            }
        }
    }

    public void flushAccounting() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "flushAccounting");
        Connection[] c = (Connection[]) connections.toArray(new Connection[connections.size()]);
        for (int i = 0; i < c.length; i++) {
            RoutingConnection rc = (RoutingConnection) c[i].getUserObject();
            if (rc != null)
                rc.flushAccounting();
        }
    }

    public void addRoute(RouteImpl route) {
        super.addRoute(route);
    }

    public void removeRoute(RouteImpl route) {
        super.removeRoute(route);
    }

    private void createRoutingQueues() throws SwiftletException {
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "createRoutingQueues/checking whether queue " + ctx.unroutableQueue + " exists ...");
            if (!ctx.queueManager.isQueueDefined(ctx.unroutableQueue)) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "createRoutingQueues/create " + ctx.unroutableQueue + " ...");
                ctx.queueManager.createQueue(ctx.unroutableQueue, (ActiveLogin) null);
            }
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }
    }

    private void createHostAccessList(ListenerMetaData meta, EntityList haEntitiy) {
        Map h = haEntitiy.getEntities();
        if (h.size() > 0) {
            for (Iterator hIter = h.keySet().iterator(); hIter.hasNext(); ) {
                String predicate = (String) hIter.next();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "Listener '" + meta + "': inbound host restrictions to: " + predicate);
                meta.addToHostAccessList(predicate);
            }
        } else if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "Listener '" + meta + "': no inbound host restrictions");

        haEntitiy.setEntityAddListener(new EntityChangeAdapter(meta) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                ListenerMetaData myMeta = (ListenerMetaData) configObject;
                String predicate = newEntity.getName();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd (host access list): listener=" + myMeta + ",new host=" + predicate);
                myMeta.addToHostAccessList(predicate);
            }
        });
        haEntitiy.setEntityRemoveListener(new EntityChangeAdapter(meta) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                ListenerMetaData myMeta = (ListenerMetaData) configObject;
                String predicate = delEntity.getName();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove (host access list): listener=" + myMeta + ",del host=" + predicate);
                myMeta.addToHostAccessList(predicate);
            }
        });
    }

    private ListenerMetaData createListener(Entity listenerEntity) throws SwiftletException {
        String listenerName = listenerEntity.getName();
        int port = ((Integer) listenerEntity.getProperty("port").getValue()).intValue();
        String socketFactoryClass = (String) listenerEntity.getProperty("socketfactory-class").getValue();
        long keepAliveInterval = ((Long) listenerEntity.getProperty("keepalive-interval").getValue()).longValue();
        Property prop = listenerEntity.getProperty("password");
        String password = (String) prop.getValue();
        InetAddress bindAddress = null;
        try {
            String s = (String) listenerEntity.getProperty("bindaddress").getValue();
            if (s != null && s.trim().length() > 0)
                bindAddress = InetAddress.getByName(s);
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }
        int inputBufferSize = ((Integer) listenerEntity.getProperty("router-input-buffer-size").getValue()).intValue();
        int inputExtendSize = ((Integer) listenerEntity.getProperty("router-input-extend-size").getValue()).intValue();
        int outputBufferSize = ((Integer) listenerEntity.getProperty("router-output-buffer-size").getValue()).intValue();
        int outputExtendSize = ((Integer) listenerEntity.getProperty("router-output-extend-size").getValue()).intValue();
        boolean useTCPNoDelay = ((Boolean) listenerEntity.getProperty("use-tcp-no-delay").getValue()).booleanValue();

        ListenerMetaData meta = new ListenerMetaData(bindAddress, port, this, keepAliveInterval, socketFactoryClass, acceptor,
                inputBufferSize, inputExtendSize, outputBufferSize, outputExtendSize, useTCPNoDelay);
        listenerEntity.setUserObject(meta);
        createHostAccessList(meta, (EntityList) listenerEntity.getEntity("host-access-list"));

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "starting listener '" + listenerName + "' ...");
        try {
            passwords.put(meta, password);
            connectionEntities.put(meta, listenerEntity);
            ctx.networkSwiftlet.createTCPListener(meta);
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }
        prop.setPropertyChangeListener(new PropertyChangeAdapter(meta) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                passwords.put(configObject, newValue);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "propertyChanged (listenerPassword): oldValue=" + oldValue + ", newValue=" + newValue);
            }
        });
        return meta;
    }

    private void createListeners(EntityList listenerList) throws SwiftletException {
        String[] inboundNames = listenerList.getEntityNames();
        if (inboundNames != null) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "creating listeners ...");
            for (int i = 0; i < inboundNames.length; i++) {
                String listenerName = inboundNames[i];
                createListener(listenerList.getEntity(listenerName));
            }
        }

        listenerList.setEntityAddListener(new EntityChangeAdapter(null) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                String name = newEntity.getName();
                try {
                    createListener(newEntity);
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "onEntityAdd (listener): listener=" + name);
                } catch (Exception e) {
                    throw new EntityAddException(e.getMessage());
                }
            }
        });
        listenerList.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                ListenerMetaData meta = (ListenerMetaData) delEntity.getUserObject();
                passwords.remove(meta);
                ctx.networkSwiftlet.removeTCPListener(meta);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove (listener): listener=" + delEntity.getName());
            }
        });
    }

    private ConnectorMetaData createConnector(Entity connectorEntity) throws SwiftletException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$routing", "starting connector: " + connectorEntity.getName());
        String host = (String) connectorEntity.getProperty("hostname").getValue();
        int port = ((Integer) connectorEntity.getProperty("port").getValue()).intValue();
        long retry = ((Long) connectorEntity.getProperty("retry-time").getValue()).longValue();
        String socketFactoryClass = (String) connectorEntity.getProperty("socketfactory-class").getValue();

        Property prop = connectorEntity.getProperty("password");
        String password = (String) prop.getValue();
        int inputBufferSize = ((Integer) connectorEntity.getProperty("router-input-buffer-size").getValue()).intValue();
        int inputExtendSize = ((Integer) connectorEntity.getProperty("router-input-extend-size").getValue()).intValue();
        int outputBufferSize = ((Integer) connectorEntity.getProperty("router-output-buffer-size").getValue()).intValue();
        int outputExtendSize = ((Integer) connectorEntity.getProperty("router-output-extend-size").getValue()).intValue();
        boolean useTCPNoDelay = ((Boolean) connectorEntity.getProperty("use-tcp-no-delay").getValue()).booleanValue();

        ConnectorMetaData meta = new ConnectorMetaData(host, port, retry, this, -1, socketFactoryClass, acceptor,
                inputBufferSize, inputExtendSize, outputBufferSize, outputExtendSize, useTCPNoDelay);
        connectorEntity.setUserObject(meta);

        passwords.put(meta, password);
        connectionEntities.put(meta, connectorEntity);

        prop.setPropertyChangeListener(new PropertyChangeAdapter(meta) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                passwords.put(configObject, newValue);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "propertyChanged (connectorPassword): oldValue=" + oldValue + ", newValue=" + newValue);
            }
        });
        prop = connectorEntity.getProperty("enabled");
        if (((Boolean) prop.getValue()).booleanValue()) {
            try {
                ctx.networkSwiftlet.createTCPConnector(meta);
            } catch (Exception e) {
                throw new SwiftletException(e.getMessage());
            }
        }
        prop.setPropertyChangeListener(new PropertyChangeAdapter(meta) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                try {
                    boolean enabled = ((Boolean) newValue).booleanValue();
                    if (enabled)
                        ctx.networkSwiftlet.createTCPConnector((ConnectorMetaData) configObject);
                    else
                        ctx.networkSwiftlet.removeTCPConnector((ConnectorMetaData) configObject);
                } catch (Exception e) {
                    throw new PropertyChangeException(e.getMessage());
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "propertyChanged (enabled): oldValue=" + oldValue + ", newValue=" + newValue);
            }
        });
        return meta;
    }

    private void createConnectors(EntityList connectorList) throws SwiftletException {
        Map m = connectorList.getEntities();
        if (m.size() > 0) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$routing", "starting connectors ...");
            for (Iterator iter = m.entrySet().iterator(); iter.hasNext(); ) {
                createConnector((Entity) ((Map.Entry) iter.next()).getValue());
            }
        }

        connectorList.setEntityAddListener(new EntityChangeAdapter(null) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                String name = newEntity.getName();
                try {
                    createConnector(newEntity);
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "onEntityAdd (connector): connector=" + name);
                } catch (Exception e) {
                    throw new EntityAddException(e.getMessage());
                }
            }
        });
        connectorList.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                ConnectorMetaData meta = (ConnectorMetaData) delEntity.getUserObject();
                passwords.remove(meta);
                ctx.networkSwiftlet.removeTCPConnector(meta);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove (connector): connector=" + delEntity.getName());
            }
        });
    }

    private void createStaticRoutes(EntityList staticRouteList) throws SwiftletException {
        String[] staticRoutes = staticRouteList.getEntityNames();
        if (staticRoutes != null) {
            for (int i = 0; i < staticRoutes.length; i++) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "creating static route to: " + staticRoutes[i]);
                try {
                    Scheduler scheduler = ctx.schedulerRegistry.getScheduler(staticRoutes[i]);
                    addRoute(new RouteImpl(staticRoutes[i], scheduler.getQueueName(), true, scheduler));
                } catch (Exception e) {
                    throw new SwiftletException(e.getMessage());
                }
            }
        }

        staticRouteList.setEntityAddListener(new EntityChangeAdapter(null) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                String dest = newEntity.getName();
                try {
                    SwiftUtilities.verifyRouterName(dest);
                    Scheduler scheduler = ctx.schedulerRegistry.getScheduler(dest);
                    RouteImpl route = (RouteImpl) getRoute(dest);
                    if (route == null)
                        addRoute(new RouteImpl(dest, scheduler.getQueueName(), false, scheduler));
                    else {
                        route.setStaticRoute(true);
                        if (route.getScheduler() == null)
                            route.setScheduler(scheduler);
                    }
                } catch (Exception e) {
                    throw new EntityAddException(e.getMessage());
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd (staticroute.routers): new staticroute=" + dest);
            }
        });
        staticRouteList.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                String dest = delEntity.getName();
                try {
                    Scheduler scheduler = ctx.schedulerRegistry.getScheduler(dest);
                    if (scheduler.getNumberConnections() == 0) {
                        scheduler.close();
                        removeRoute(getRoute(dest));
                    }
                } catch (Exception e) {
                    throw new EntityRemoveException(e.getMessage());
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove (staticroute.routers): del staticroute=" + dest);
            }
        });
    }

    protected SwiftletContext createSwiftletContext(RoutingSwiftletImpl routingSwiftletImpl, Entity rootEntity) throws SwiftletException {
        return new SwiftletContext(routingSwiftletImpl, rootEntity);
    }

    protected void startup(Configuration config) throws SwiftletException {
        this.config = config;
        root = config;
        passwords = Collections.synchronizedMap(new HashMap());
        connectionEntities = Collections.synchronizedMap(new HashMap());
        connections = Collections.synchronizedSet(new HashSet());
        ctx = createSwiftletContext(this, root);

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup ...");

        acceptor = new Acceptor();
        createRoutingQueues();
        createStaticRoutes((EntityList) root.getEntity("static-routes"));

        createListeners((EntityList) root.getEntity("listeners"));
        createConnectors((EntityList) root.getEntity("connectors"));
        SwiftletManager.getInstance().addSwiftletManagerListener("sys$scheduler", new SwiftletManagerAdapter() {
            public void swiftletStarted(SwiftletManagerEvent event) {
                ctx.schedulerSwiftlet = (SchedulerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$scheduler");
                jobRegistrar = new JobRegistrar(ctx);
                jobRegistrar.register();
            }

            public void swiftletStopInitiated(SwiftletManagerEvent event) {
                jobRegistrar.unregister();
            }
        });
        sourceFactory = new RoutingSourceFactory(ctx);

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup done");
    }

    protected void shutdown() throws SwiftletException {
        // true when shutdown while standby
        if (ctx == null)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown ...");

        removeAllRoutes();
        removeAllRoutingListeners();

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "stopping connectors ...");
        EntityList connectorList = (EntityList) root.getEntity("connectors");
        String[] outboundNames = connectorList.getEntityNames();
        if (outboundNames != null) {
            for (int i = 0; i < outboundNames.length; i++) {
                Entity entity = connectorList.getEntity(outboundNames[i]);
                if (((Boolean) entity.getProperty("enabled").getValue()).booleanValue()) {
                    ConnectorMetaData meta = (ConnectorMetaData) entity.getUserObject();
                    ctx.networkSwiftlet.removeTCPConnector(meta);
                }
            }
        }

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "stopping listeners ...");
        EntityList listenerList = (EntityList) root.getEntity("listeners");
        String[] inboundNames = listenerList.getEntityNames();
        if (inboundNames != null) {
            for (int i = 0; i < inboundNames.length; i++) {
                ListenerMetaData meta = (ListenerMetaData) listenerList.getEntity(inboundNames[i]).getUserObject();
                ctx.networkSwiftlet.removeTCPListener(meta);
            }
        }

        Connection[] c = (Connection[]) connections.toArray(new Connection[connections.size()]);
        if (c.length > 0)
            shutdownSem = new Semaphore();
        for (int i = 0; i < c.length; i++) {
            ctx.networkSwiftlet.getConnectionManager().removeConnection(c[i]);
        }
        if (shutdownSem != null) {
            System.out.println("+++ Waiting for Connection Termination ...");
            shutdownSem.waitHere();
            shutdownSem = null;
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "stopping connection manager ...");
        Semaphore sem = new Semaphore();
        ctx.connectionManager.enqueue(new PORemoveAllObject(null, sem));
        sem.waitHere(60000);
        ctx.connectionManager.close();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "stopping schedulers ...");
        ctx.schedulerRegistry.close();

        passwords = null;
        connectionEntities = null;
        connections = null;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown done");
        ctx = null;
    }

    private class Acceptor implements com.swiftmq.swiftlet.net.event.ConnectionListener {
        Acceptor() {
            ctx.usageList.getEntity("connections").setEntityRemoveListener(new EntityChangeAdapter(null) {
                public void onEntityRemove(Entity parent, Entity delEntity)
                        throws EntityRemoveException {
                    RoutingConnection myConnection = (RoutingConnection) delEntity.getDynamicObject();
                    ConnectionManager connectionManager = ctx.networkSwiftlet.getConnectionManager();
                    connectionManager.removeConnection(myConnection.getConnection());
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "onEntityRemove (RoutingConnection): " + myConnection);
                }
            });
        }

        public void connected(Connection connection) throws ConnectionVetoException {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "Acceptor/connected: " + connection);
            try {
                String password = (String) passwords.get(connection.getMetaData());
                Entity entity = (Entity) connectionEntities.get(connection.getMetaData());
                RoutingConnection c = new RoutingConnection(ctx, connection, entity, password);
                connection.setUserObject(c);
                connections.add(connection);
            } catch (Exception e) {
                throw new ConnectionVetoException(e.getMessage());
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "Acceptor/connected: " + connection + ", DONE.");
        }

        public void disconnected(Connection connection) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "Acceptor/disconnected: " + connection);
            RoutingConnection routingConnection = (RoutingConnection) connection.getUserObject();
            if (routingConnection != null) {
                Semaphore sem = new Semaphore();
                ctx.connectionManager.enqueue(new PORemoveObject(null, sem, routingConnection));
                sem.waitHere();
            }
            connections.remove(connection);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "Acceptor/disconnected: " + connection + ", DONE.");
            if (connections.size() == 0 && shutdownSem != null)
                shutdownSem.notifySingleWaiter();
        }
    }
}
