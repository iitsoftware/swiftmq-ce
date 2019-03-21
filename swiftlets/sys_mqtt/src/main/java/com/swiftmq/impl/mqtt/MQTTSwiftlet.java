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
import com.swiftmq.mgmt.*;
import com.swiftmq.mqtt.v311.PacketDecoder;
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
import java.util.*;

public class MQTTSwiftlet extends Swiftlet implements TimerListener, MgmtListener {
    SwiftletContext ctx = null;
    EntityListEventAdapter listenerAdapter = null;
    Semaphore shutdownSem = null;
    Set connections = Collections.synchronizedSet(new HashSet());
    boolean collectOn = false;
    long collectInterval = -1;
    long lastCollect = System.currentTimeMillis();

    private void collectChanged(long oldInterval, long newInterval) {
        if (!collectOn)
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
        Connection[] c = (Connection[]) connections.toArray(new Connection[connections.size()]);
        for (int i = 0; i < c.length; i++) {
            MQTTConnection vc = (MQTTConnection) c[i].getUserObject();
            vc.collect(lastCollect);
        }
        lastCollect = System.currentTimeMillis();
    }

    public void adminToolActivated() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "adminToolActivated");
        collectOn = true;
        collectChanged(-1, collectInterval);
    }

    public void adminToolDeactivated() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "adminToolDeactivated");
        collectChanged(collectInterval, -1);
        collectOn = false;
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
        int port = ((Integer) listenerEntity.getProperty("port").getValue()).intValue();
        InetAddress bindAddress = null;
        try {
            String s = (String) listenerEntity.getProperty("bindaddress").getValue();
            if (s != null && s.trim().length() > 0)
                bindAddress = InetAddress.getByName(s);
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }

        int inputBufferSize = ((Integer) connectionTemplate.getProperty("router-input-buffer-size").getValue()).intValue();
        int inputExtendSize = ((Integer) connectionTemplate.getProperty("router-input-extend-size").getValue()).intValue();
        int outputBufferSize = ((Integer) connectionTemplate.getProperty("router-output-buffer-size").getValue()).intValue();
        int outputExtendSize = ((Integer) connectionTemplate.getProperty("router-output-extend-size").getValue()).intValue();
        boolean useTCPNoDelay = ((Boolean) connectionTemplate.getProperty("use-tcp-no-delay").getValue()).booleanValue();
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

    public Entity getConnectionTemplate(String name) {
        Entity entity = ctx.root.getEntity("declarations").getEntity("connection-templates").getEntity(name);
        if (entity == null)
            entity = ((EntityList) ctx.root.getEntity("declarations").getEntity("connection-templates")).getTemplate();
        return entity;
    }

    protected synchronized Semaphore getShutdownSemaphore() {
        shutdownSem = null;
        if (connections.size() > 0)
            shutdownSem = new Semaphore();
        return shutdownSem;
    }

    private void doConnect(Connection connection, Entity connectionTemplate) {
        Entity ce = ctx.usageListConnections.createEntity();
        MQTTConnection mqttConnection = new MQTTConnection(ctx, connection, ce, connectionTemplate);
        PacketDecoder packetDecoder = new PacketDecoder(mqttConnection, ctx.protSpace, ((Integer) connectionTemplate.getProperty("max-message-size").getValue()).intValue());
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
            if (shutdownSem != null && connections.size() == 0)
                shutdownSem.notifySingleWaiter();
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
                collectInterval = ((Long) newValue).longValue();
                collectChanged(((Long) oldValue).longValue(), collectInterval);
            }
        });
        collectInterval = ((Long) prop.getValue()).longValue();
        if (collectOn) {
            if (collectInterval > 0) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "startup: registering msg/s count collector");
                ctx.timerSwiftlet.addTimerListener(collectInterval, this);
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
            collectChanged(collectInterval, -1);
            listenerAdapter.close();
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown: shutdown all MQTT connections");
            Semaphore sem = getShutdownSemaphore();
            ConnectionManager connectionManager = ctx.networkSwiftlet.getConnectionManager();
            Connection[] c = (Connection[]) connections.toArray(new Connection[connections.size()]);
            connections.clear();
            for (int i = 0; i < c.length; i++) {
                connectionManager.removeConnection(c[i]);
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
        int localMax = -1;
        int currentCount = 0;

        Acceptor(String name, Property maxConnProp, Property connectionTemplateProp) {
            this.name = name;
            this.connectionTemplate = getConnectionTemplate((String) connectionTemplateProp.getValue());
            connectionTemplateProp.setPropertyChangeListener(new PropertyChangeListener() {
                public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                    connectionTemplate = getConnectionTemplate((String) newValue);
                }
            });
            if (maxConnProp != null) {
                localMax = ((Integer) maxConnProp.getValue()).intValue();
                maxConnProp.setPropertyChangeListener(new PropertyChangeListener() {
                    public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                        synchronized (Acceptor.this) {
                            localMax = ((Integer) newValue).intValue();
                        }
                    }
                });
            }
        }

        public synchronized void connected(Connection connection) throws ConnectionVetoException {
            if (localMax != -1) {
                currentCount++;
                if (currentCount > localMax)
                    throw new ConnectionVetoException("Maximum connections (" + localMax + ") for this listener '" + name + "' reached!");
            }
            doConnect(connection, connectionTemplate);
        }

        public synchronized void disconnected(Connection connection) {
            doDisconnect(connection);
            if (localMax != -1) {
                currentCount--;
            }
        }
    }
}
