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

package com.swiftmq.impl.mqtt;

import com.swiftmq.impl.mqtt.connection.MQTTConnection;
import com.swiftmq.impl.mqtt.v311.PacketDecoder;
import com.swiftmq.mgmt.*;
import com.swiftmq.net.protocol.raw.RawInputHandler;
import com.swiftmq.net.protocol.raw.RawOutputHandler;
import com.swiftmq.swiftlet.Swiftlet;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.mgmt.event.MgmtListener;
import com.swiftmq.swiftlet.net.Connection;
import com.swiftmq.swiftlet.net.ConnectionManager;
import com.swiftmq.swiftlet.net.ConnectionVetoException;
import com.swiftmq.swiftlet.net.ListenerMetaData;
import com.swiftmq.swiftlet.net.event.ConnectionListener;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.concurrent.Semaphore;

import java.net.InetAddress;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class MQTTSwiftlet extends Swiftlet implements TimerListener, MgmtListener {
    SwiftletContext ctx = null;
    EntityListEventAdapter listenerAdapter = null;
    final AtomicReference<Semaphore> shutdownSem = new AtomicReference<>();
    Set<Connection> connections = ConcurrentHashMap.newKeySet();
    final AtomicBoolean collectOn = new AtomicBoolean(false);
    final AtomicLong collectInterval = new AtomicLong(-1);
    final AtomicLong lastCollect = new AtomicLong(System.currentTimeMillis());

    private void collectChanged(long oldInterval, long newInterval) {
        if (!collectOn.get())
            return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "collectChanged: old interval: " + oldInterval + " new interval: " + newInterval);
        if (oldInterval > 0) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "collectChanged: removeTimerListener for interval " + oldInterval);
            ctx.timerSwiftlet.removeTimerListener(this);
        }
        if (newInterval > 0) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "collectChanged: addTimerListener for interval " + newInterval);
            ctx.timerSwiftlet.addTimerListener(newInterval, this);
        }
    }

    public void performTimeAction() {
        Connection[] c = connections.toArray(new Connection[0]);
        for (Connection connection : c) {
            MQTTConnection vc = (MQTTConnection) connection.getUserObject();
            vc.collect(lastCollect.get());
        }
        lastCollect.set(System.currentTimeMillis());
    }

    public void adminToolActivated() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "adminToolActivated");
        collectOn.set(true);
        collectChanged(-1, collectInterval.get());
    }

    public void adminToolDeactivated() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "adminToolDeactivated");
        collectChanged(collectInterval.get(), -1);
        collectOn.set(false);
    }

    private void createListenerAdapter(EntityList listenerList) throws SwiftletException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createListenerAdapter ...");
        listenerAdapter = new EntityListEventAdapter(listenerList, true, true) {
            public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd: " + newEntity.getName() + " ...");
                try {
                    createListener(newEntity);
                } catch (SwiftletException e) {
                    throw new EntityAddException(e.toString());
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd: " + newEntity.getName() + " done");
            }

            public void onEntityRemove(Entity parent, Entity delEntity) throws EntityRemoveException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove: " + delEntity.getName() + " ...");
                try {
                    ctx.networkSwiftlet.removeTCPListener((ListenerMetaData) delEntity.getUserObject());
                } catch (Exception e) {
                    throw new EntityRemoveException(e.getMessage());
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove: " + delEntity.getName() + " done");
            }
        };
        try {
            listenerAdapter.init();
        } catch (Exception e) {
            throw new SwiftletException(e.toString());
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createListenerAdapter done");
    }

    private void createListener(Entity listenerEntity) throws SwiftletException {
        Entity connectionTemplate = getConnectionTemplate((String) listenerEntity.getProperty("connection-template").getValue());
        String listenerName = listenerEntity.getName();
        int port = (Integer) listenerEntity.getProperty("port").getValue();
        InetAddress bindAddress = null;
        try {
            String s = (String) listenerEntity.getProperty("bindaddress").getValue();
            if (s != null && !s.trim().isEmpty())
                bindAddress = InetAddress.getByName(s);
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }

        int inputBufferSize = (Integer) connectionTemplate.getProperty("router-input-buffer-size").getValue();
        int inputExtendSize = (Integer) connectionTemplate.getProperty("router-input-extend-size").getValue();
        int outputBufferSize = (Integer) connectionTemplate.getProperty("router-output-buffer-size").getValue();
        int outputExtendSize = (Integer) connectionTemplate.getProperty("router-output-extend-size").getValue();
        boolean useTCPNoDelay = (Boolean) connectionTemplate.getProperty("use-tcp-no-delay").getValue();
        ListenerMetaData meta = new ListenerMetaData(bindAddress, port, this, -1, (String) connectionTemplate.getProperty("socketfactory-class").getValue(), new Acceptor(listenerName, listenerEntity.getProperty("max-connections"), listenerEntity.getProperty("connection-template")),
                inputBufferSize, inputExtendSize, outputBufferSize, outputExtendSize, useTCPNoDelay, new RawInputHandler(), new RawOutputHandler());
        listenerEntity.setUserObject(meta);
        createHostAccessList(meta, (EntityList) listenerEntity.getEntity("host-access-list"));
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "starting listener '" + listenerName + "' ...");
        try {
            ctx.networkSwiftlet.createTCPListener(meta);
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }
    }

    private void createHostAccessList(ListenerMetaData meta, EntityList haEntitiy) {
        Map h = haEntitiy.getEntities();
        if (!h.isEmpty()) {
            for (Object o : h.keySet()) {
                String predicate = (String) o;
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

    public Entity getConnectionTemplate(String name) {
        Entity entity = ctx.root.getEntity("declarations").getEntity("connection-templates").getEntity(name);
        if (entity == null)
            entity = ((EntityList) ctx.root.getEntity("declarations").getEntity("connection-templates")).getTemplate();
        return entity;
    }

    protected Semaphore getShutdownSemaphore() {
        if (!connections.isEmpty()) {
            shutdownSem.set(new Semaphore());
        } else {
            shutdownSem.set(null);
        }
        return shutdownSem.get();
    }

    private void doConnect(Connection connection, Entity connectionTemplate) {
        Entity ce = ctx.usageListConnections.createEntity();
        MQTTConnection mqttConnection = new MQTTConnection(ctx, connection, ce, connectionTemplate);
        PacketDecoder packetDecoder = new PacketDecoder(mqttConnection, ctx.protSpace, (Integer) connectionTemplate.getProperty("max-message-size").getValue());
        connection.setInboundHandler(packetDecoder);
        connection.setUserObject(mqttConnection);
        connections.add(connection);
        try {
            ce.setName(connection.toString());
            ce.getProperty("connect-time").setValue(new Date().toString());
            ce.setDynamicObject(connection);
            ce.createCommands();
            ctx.usageListConnections.addEntity(ce);
        } catch (Exception ignored) {
        }
    }

    private void doDisconnect(Connection connection) {
        // It may happen during shutdown that the Network Swiftlet calls this method and ctx becomes null
        SwiftletContext myCtx = ctx;
        if (myCtx == null)
            return;
        if (myCtx.traceSpace.enabled) myCtx.traceSpace.trace(getName(), "doDisconnect: " + connection);
        MQTTConnection mqttConnection = (MQTTConnection) connection.getUserObject();
        if (mqttConnection != null) {
            myCtx.usageListConnections.removeDynamicEntity(connection);
            mqttConnection.close();
            connections.remove(connection);
            if (shutdownSem.get() != null && connections.size() == 0)
                shutdownSem.get().notifySingleWaiter();
        }
        if (myCtx.traceSpace.enabled) myCtx.traceSpace.trace(getName(), "doDisconnect: " + connection + ", DONE.");
    }

    @Override
    protected void startup(Configuration config) throws SwiftletException {
        try {
            ctx = new SwiftletContext(config, this);
        } catch (Exception e) {
            e.printStackTrace();
            throw new SwiftletException(e.getMessage());
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup ...");


        createListenerAdapter((EntityList) ctx.root.getEntity("listeners"));

        Property prop = ctx.root.getProperty("collect-interval");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                collectInterval.set((Long) newValue);
                collectChanged((Long) oldValue, collectInterval.get());
            }
        });
        collectInterval.set((Long) prop.getValue());
        if (collectOn.get()) {
            if (collectInterval.get() > 0) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "startup: registering msg/s count collector");
                ctx.timerSwiftlet.addTimerListener(collectInterval.get(), this);
            } else if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "startup: collect interval <= 0; no msg/s count collector");
        }

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "registering MgmtListener ...");
        ctx.mgmtSwiftlet.addMgmtListener(this);

        ctx.usageListConnections.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                Connection myConnection = (Connection) delEntity.getDynamicObject();
                ConnectionManager connectionManager = ctx.networkSwiftlet.getConnectionManager();
                connectionManager.removeConnection(myConnection);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove (Connection): " + myConnection);
            }
        });

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup done.");
    }

    @Override
    protected void shutdown() throws SwiftletException {
        // true when shutdown while standby
        if (ctx == null)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown ...");
        try {
            collectChanged(collectInterval.get(), -1);
            listenerAdapter.close();
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown: shutdown all MQTT connections");
            Semaphore sem = getShutdownSemaphore();
            ConnectionManager connectionManager = ctx.networkSwiftlet.getConnectionManager();
            Connection[] c = connections.toArray(new Connection[0]);
            connections.clear();
            for (Connection connection : c) {
                connectionManager.removeConnection(connection);
            }
            if (sem != null) {
                System.out.println("+++ waiting for connection shutdown ...");
                sem.waitHere();
                try {
                    Thread.sleep(5000);
                } catch (Exception ignored) {
                }
            }
            ctx.mgmtSwiftlet.removeMgmtListener(this);
            ctx.sessionRegistry.close();
            ctx.retainer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown done.");
        ctx = null;
    }

    private class Acceptor implements ConnectionListener {
        String name = null;
        Entity connectionTemplate = null;
        final AtomicInteger localMax = new AtomicInteger(-1);
        final AtomicInteger currentCount = new AtomicInteger();

        Acceptor(String name, Property maxConnProp, Property connectionTemplateProp) {
            this.name = name;
            this.connectionTemplate = getConnectionTemplate((String) connectionTemplateProp.getValue());
            connectionTemplateProp.setPropertyChangeListener((property, oldValue, newValue) -> connectionTemplate = getConnectionTemplate((String) newValue));
            if (maxConnProp != null) {
                localMax.set((Integer) maxConnProp.getValue());
                maxConnProp.setPropertyChangeListener((property, oldValue, newValue) -> {
                    localMax.set((Integer) newValue);
                });
            }
        }

        public void connected(Connection connection) throws ConnectionVetoException {
            if (localMax.get() != -1 && currentCount.incrementAndGet() > localMax.get()) {
                currentCount.decrementAndGet();
                throw new ConnectionVetoException("Maximum connections (" + localMax.get() + ") for this listener '" + name + "' reached!");
            }
            doConnect(connection, connectionTemplate);
        }

        public void disconnected(Connection connection) {
            doDisconnect(connection);
            if (localMax.get() != -1) {
                currentCount.decrementAndGet();
            }
        }
    }
}
