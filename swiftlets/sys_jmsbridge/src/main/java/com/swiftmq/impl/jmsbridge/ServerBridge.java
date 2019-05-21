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

package com.swiftmq.impl.jmsbridge;

import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.timer.event.TimerListener;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import java.util.*;

public class ServerBridge implements TimerListener, ExceptionListener {
    SwiftletContext ctx = null;
    Entity serverEntity = null;
    Entity usageEntity = null;
    String name = null;
    String tracePrefix = null;
    ObjectFactory objectFactory = null;
    ConnectionCache connectionCache = null;
    long retryInterval = 0;
    volatile boolean destroyInProgress = false;
    volatile boolean connected = false;

    Map bridges = Collections.synchronizedMap(new HashMap());

    ServerBridge(SwiftletContext ctx, Entity serverEntity) throws Exception {
        this.ctx = ctx;
        this.serverEntity = serverEntity;
        name = serverEntity.getName();
        tracePrefix = "xt$bridge/" + name;
        Property prop = serverEntity.getProperty("enabled");
        boolean enabled = ((Boolean) prop.getValue()).booleanValue();
        /*${evaltimer}*/

        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                try {
                    if (((Boolean) newValue).booleanValue()) {
                        connect();
                        startTimer();
                    } else {
                        stopTimer();
                        destroy();
                    }
                } catch (Exception e) {
                    throw new PropertyChangeException(e.toString());
                }
            }
        });

        if (enabled) {
            connect();
            startTimer();
        }
    }

    private void startTimer() {
        retryInterval = ((Long) serverEntity.getProperty("retryinterval").getValue()).longValue();
        if (retryInterval > 0)
            ctx.timerSwiftlet.addTimerListener(retryInterval, this);
    }

    private void stopTimer() {
        retryInterval = ((Long) serverEntity.getProperty("retryinterval").getValue()).longValue();
        if (retryInterval > 0)
            ctx.timerSwiftlet.removeTimerListener(this);
    }

    public String getName() {
        return name;
    }

    public void performTimeAction() {
        if (!connected) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "performTimeAction, connect ...");
            try {
                connect();
            } catch (Exception ignored) {
            }
        }
    }

    public void onException(JMSException e) {
        ctx.logSwiftlet.logError("xt$bridge", name + "/onException: " + e);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, name + "/onException: " + e);
        ctx.bridgeSwiftlet.destroyServer(name);
    }

    public DestinationBridge getBridging(String name) {
        DestinationBridge b = null;
        synchronized (bridges) {
            b = (DestinationBridge) bridges.get(name);
        }
        return b;
    }

    private synchronized void connect() throws Exception {
        Entity ofEntity = serverEntity.getEntity("objectfactory");
        String s = (String) ofEntity.getProperty("class").getValue();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "creating objectfactory '" + s + "'");
        objectFactory = (ObjectFactory) Class.forName(s).newInstance();

        Entity propEntity = ofEntity.getEntity("properties");
        Map m = propEntity.getEntities();
        if (m != null) {
            Properties prop = new Properties();
            for (Iterator iter = m.entrySet().iterator(); iter.hasNext(); ) {
                Entity pe = (Entity) ((Map.Entry) iter.next()).getValue();
                prop.setProperty(pe.getName(), (String) pe.getProperty("value").getValue());
            }
            objectFactory.setProperties(prop);
        }
        String userName = (String) serverEntity.getProperty("username").getValue();
        String password = (String) serverEntity.getProperty("password").getValue();
        connectionCache = new ConnectionCache(objectFactory, userName, password, this);

        createBridgings((EntityList) serverEntity.getEntity("bridgings"));
    }

    private void createBridgings(EntityList bridgingsList) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "create bridgings ...");
        connected = true;
        String[] s = bridgingsList.getEntityNames();
        if (s != null) {
            int i = 0;
            try {
                usageEntity = ctx.usage.createEntity();
                usageEntity.setName(name);
                usageEntity.setDynamicObject(this);
                usageEntity.createCommands();
                ctx.usage.addEntity(usageEntity);
                Property prop = usageEntity.getProperty("connecttime");
                prop.setValue("DISCONNECTED");
                for (i = 0; i < s.length; i++) {
                    createBridging(bridgingsList.getEntity(s[i]));
                }
                prop.setValue(new Date().toString());
            } catch (Exception e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(tracePrefix, "Exception creating bridging '" + s[i] + "': " + e);
                ctx.logSwiftlet.logError(tracePrefix, "Exception creating bridging '" + s[i] + "': " + e);
                destroyBridgings((EntityList) serverEntity.getEntity("bridgings"));
                if (connectionCache != null)
                    connectionCache.closeAll();
                try {
                    objectFactory.destroy();
                } catch (Exception ignored) {
                }
                connected = false;
                ctx.usage.removeEntity(usageEntity);
            }
        } else if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "no bridgings defined");

        if (bridgingsList.getEntityAddListener() != null)
            return;

        bridgingsList.setEntityAddListener(new EntityChangeAdapter(null) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                String name = newEntity.getName();
                try {
                    createBridging(newEntity);
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(tracePrefix, "onEntityAdd (bridgings): bridging=" + name);
                } catch (Exception e) {
                    String msg = "onEntityAdd (bridgings): Exception creating bridging '" + name + "': " + e;
                    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, msg);
                    ctx.logSwiftlet.logError(tracePrefix, msg);
                    throw new EntityAddException(msg);
                }
            }
        });
        bridgingsList.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                String name = delEntity.getName();
                destroyBridging(delEntity);
                bridges.remove(name);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(tracePrefix, "onEntityRemove (bridgings): bridging=" + name);
            }
        });
    }

    private void createBridging(Entity bridgingEntity) throws Exception {
        String name = bridgingEntity.getName();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "create bridging '" + name + "' ...");
        String clientId = (String) serverEntity.getProperty("clientid").getValue();
        DestinationBridge bridge = new DestinationBridge(ctx, this, bridgingEntity, (EntityList) usageEntity.getEntity("active-bridgings"), connectionCache, clientId, tracePrefix);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "created: " + bridge);
        bridges.put(name, bridge);
    }

    private void destroyBridgings(EntityList bridgingsList) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "destroy bridgings ...");
        String[] s = bridgingsList.getEntityNames();
        if (s != null) {
            for (int i = 0; i < s.length; i++) {
                destroyBridging(bridgingsList.getEntity(s[i]));
            }
        } else if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "no bridgings defined");
    }

    private void destroyBridging(Entity bridgingEntity) {
        String name = bridgingEntity.getName();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "destroy bridging '" + name + "'");
        DestinationBridge bridge = (DestinationBridge) bridges.remove(name);
        if (bridge != null)
            bridge.destroy();
    }

    void collect() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "collect ...");
        synchronized (bridges) {
            for (Iterator iter = bridges.entrySet().iterator(); iter.hasNext(); ) {
                DestinationBridge bridge = (DestinationBridge) ((Map.Entry) iter.next()).getValue();
                bridge.collect();
            }
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "collect done");
    }

    boolean isDestroyInProgress() {
        return destroyInProgress;
    }

    synchronized void destroy() {
        destroyInProgress = true;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "destroying ...");
        destroyBridgings((EntityList) serverEntity.getEntity("bridgings"));
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "closing all connections");
        if (connectionCache != null)
            connectionCache.closeAll();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "destroying object factory");
        try {
            objectFactory.destroy();
        } catch (Exception ignored) {
        }
        connected = false;
        destroyInProgress = false;
        if (usageEntity != null) {
            try {
                ctx.usage.removeEntity(usageEntity);
            } catch (EntityRemoveException e) {
                e.printStackTrace();
            }
            usageEntity = null;
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "destroying - finished");
    }

    void close() {
        if (retryInterval > 0)
            ctx.timerSwiftlet.removeTimerListener(this);
    }
}

