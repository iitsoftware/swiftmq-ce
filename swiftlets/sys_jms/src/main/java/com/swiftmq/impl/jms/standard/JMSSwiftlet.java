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

package com.swiftmq.impl.jms.standard;

import com.swiftmq.auth.ChallengeResponseFactory;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.Swiftlet;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.mgmt.event.MgmtListener;
import com.swiftmq.swiftlet.net.*;
import com.swiftmq.swiftlet.net.event.ConnectionListener;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.versioning.Versionable;
import com.swiftmq.tools.versioning.Versioned;

import javax.jms.DeliveryMode;
import javax.jms.InvalidClientIDException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class JMSSwiftlet extends Swiftlet implements TimerListener, MgmtListener {
    public static final String TP_CONNSVC = "sys$jms.connection.service";
    static final String INTRAVM_LISTENER = "intravm";

    protected SwiftletContext ctx = null;
    ChallengeResponseFactory challengeResponseFactory = null;
    Set<String> clientSet = ConcurrentHashMap.newKeySet();
    final AtomicInteger maxConnections = new AtomicInteger(-1);
    Set<Connection> connections = ConcurrentHashMap.newKeySet();
    Map<String, String> connectAddresses = new ConcurrentHashMap<>();
    IntraVMListenerMetaData intraVMMetaData = null;
    final AtomicBoolean collectOn = new AtomicBoolean(false);
    final AtomicLong collectInterval = new AtomicLong(-1);
    final AtomicLong lastCollect = new AtomicLong(System.currentTimeMillis());
    final AtomicReference<Semaphore> shutdownSem = new AtomicReference<>();
    final AtomicBoolean allowSameClientId = new AtomicBoolean(false);

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
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "performTimeAction ...");
        Connection[] c = connections.toArray(new Connection[0]);
        for (Connection connection : c) {
            VersionSelector vs = (VersionSelector) connection.getUserObject();
            if (vs != null) {
                VersionedJMSConnection jmsc = vs.getJmsConnection();
                if (jmsc != null) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "performTimeAction, collect on: " + jmsc + ", lastCollect: " + lastCollect);
                    jmsc.collect(lastCollect.get());
                }
            }
        }
        lastCollect.set(System.currentTimeMillis());
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "performTimeAction done");
    }

    public ChallengeResponseFactory getChallengeResponseFactory() {
        return challengeResponseFactory;
    }

    public void addClientId(String clientId) throws InvalidClientIDException {
        if (!allowSameClientId.get()) {
            boolean added = clientSet.add(clientId);
            if (!added) {
                throw new InvalidClientIDException("clientId '" + clientId + "' is already in use");
            }
        }
    }

    public void removeClientId(String clientId) {
        if (!allowSameClientId.get())
            clientSet.remove(clientId);
    }

    private void createConnectionFactory(Entity listener, Entity cfEntity) throws Exception {
        String hostname = null;
        try {
            hostname = InetAddress.getByName(InetAddress.getLocalHost().getHostAddress()).getHostName();
        } catch (UnknownHostException e) {
        }
        int port = (Integer) listener.getProperty("port").getValue();
        String socketFactoryClass = (String) listener.getProperty("socketfactory-class").getValue();
        long keepAliveInterval = (Long) listener.getProperty("keepalive-interval").getValue();
        String bindAddress = (String) listener.getProperty("bindaddress").getValue();
        if (bindAddress != null && bindAddress.trim().isEmpty())
            bindAddress = null;
        String connectIP = (String) listener.getProperty("connectaddress").getValue();
        if (connectIP != null && connectIP.trim().isEmpty())
            connectIP = null;
        int connectPort = (Integer) listener.getProperty("connectport").getValue();
        int connectPort2 = (Integer) listener.getProperty("connectport2").getValue();
        String clientId = (String) cfEntity.getProperty("jms-client-id").getValue();
        if (clientId != null && clientId.trim().isEmpty())
            clientId = null;
        int smqpProducerReplyInterval = (Integer) cfEntity.getProperty("smqp-producer-reply-interval").getValue();
        int smqpConsumerCacheSize = (Integer) cfEntity.getProperty("smqp-consumer-cache-size").getValue();
        int smqpConsumerCacheSizeKB = (Integer) cfEntity.getProperty("smqp-consumer-cache-size-kb").getValue();
        String s = ((String) cfEntity.getProperty("jms-default-delivery-mode").getValue()).toLowerCase();
        int jmsDeliveryMode = s.equals("persistent") ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
        int jmsPriority = (Integer) cfEntity.getProperty("jms-default-message-priority").getValue();
        long jmsTTL = (Long) cfEntity.getProperty("jms-default-message-ttl").getValue();
        boolean jmsMessageIdEnabled = (Boolean) cfEntity.getProperty("jms-default-message-id-enabled").getValue();
        boolean jmsMessageTimestampEnabled = (Boolean) cfEntity.getProperty("jms-default-message-timestamp-enabled").getValue();
        boolean useThreadContextCL = (Boolean) cfEntity.getProperty("thread-context-classloader-for-getobject").getValue();
        int inputBufferSize = (Integer) cfEntity.getProperty("client-input-buffer-size").getValue();
        int inputExtendSize = (Integer) cfEntity.getProperty("client-input-extend-size").getValue();
        int outputBufferSize = (Integer) cfEntity.getProperty("client-output-buffer-size").getValue();
        int outputExtendSize = (Integer) cfEntity.getProperty("client-output-extend-size").getValue();

        String hostname2 = (String) listener.getProperty("hostname2").getValue();
        int port2 = (Integer) listener.getProperty("port2").getValue();
        String bindAddress2 = (String) listener.getProperty("bindaddress2").getValue();
        if (bindAddress2 != null && bindAddress2.trim().isEmpty())
            bindAddress2 = null;
        String connectIP2 = (String) listener.getProperty("connectaddress2").getValue();
        if (connectIP2 != null && connectIP2.trim().isEmpty())
            connectIP2 = null;
        boolean reconnectEnabled = (Boolean) cfEntity.getProperty("reconnect-enabled").getValue();
        int maxRetries = (Integer) cfEntity.getProperty("reconnect-max-retries").getValue();
        long retryDelay = (Long) cfEntity.getProperty("reconnect-delay").getValue();
        boolean duplicateMessageDetection = (Boolean) cfEntity.getProperty("duplicate-message-detection").getValue();
        int duplicateBacklogSize = (Integer) cfEntity.getProperty("duplicate-backlog-size").getValue();

        com.swiftmq.jms.v750.ConnectionFactoryImpl cf7 = new com.swiftmq.jms.v750.ConnectionFactoryImpl(listener.getName(),
                socketFactoryClass,
                connectIP != null ? connectIP : bindAddress != null ? bindAddress : hostname,
                connectPort != -1 ? connectPort : port,
                keepAliveInterval,
                clientId,
                smqpProducerReplyInterval,
                smqpConsumerCacheSize,
                smqpConsumerCacheSizeKB,
                jmsDeliveryMode,
                jmsPriority,
                jmsTTL,
                jmsMessageIdEnabled,
                jmsMessageTimestampEnabled,
                useThreadContextCL,
                inputBufferSize,
                inputExtendSize,
                outputBufferSize,
                outputExtendSize,
                false);
        cf7.setHostname2(connectIP2 != null ? connectIP2 : bindAddress2 != null ? bindAddress2 : hostname2);
        cf7.setPort2(connectPort2 != -1 ? connectPort2 : port2);
        cf7.setReconnectEnabled(reconnectEnabled);
        cf7.setMaxRetries(maxRetries);
        cf7.setRetryDelay(retryDelay);
        cf7.setDuplicateMessageDetection(duplicateMessageDetection);
        cf7.setDuplicateBacklogSize(duplicateBacklogSize);
        DataByteArrayOutputStream dos7 = new DataByteArrayOutputStream();
        dos7.writeInt(cf7.getDumpId());
        cf7.writeContent(dos7);

        Versionable versionable = new Versionable();
        versionable.addVersioned(750, new Versioned(750, dos7.getBuffer(), dos7.getCount()), "com.swiftmq.jms.v750.CFFactory");
        ctx.jndiSwiftlet.registerJNDIObject(cfEntity.getName(), versionable);
    }

    private void createConnectionFactories(Entity listener, EntityList cfList) throws Exception {
        Map map = cfList.getEntities();
        if (map != null) {
            for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); ) {
                createConnectionFactory(listener, (Entity) ((Map.Entry) iter.next()).getValue());
            }
        }
        cfList.setEntityAddListener(new EntityChangeAdapter(listener) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                try {
                    Entity myListener = (Entity) configObject;
                    createConnectionFactory(myListener, newEntity);
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "onEntityAdd (connection factory): listener=" + myListener.getName() + ",new cf=" + newEntity.getName());
                } catch (Exception e) {
                    throw new EntityAddException(e.toString());
                }
            }
        });
        cfList.setEntityRemoveListener(new EntityChangeAdapter(listener) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                Entity myListener = (Entity) configObject;
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd (connection factory): listener=" + myListener.getName() + ",del cf=" + delEntity.getName());
                ctx.jndiSwiftlet.deregisterJNDIObject(delEntity.getName());
            }
        });
    }

    private void createIVMConnectionFactory(Entity cfEntity) throws Exception {
        String clientId = (String) cfEntity.getProperty("jms-client-id").getValue();
        if (clientId != null && clientId.trim().isEmpty())
            clientId = null;
        int smqpProducerReplyInterval = (Integer) cfEntity.getProperty("smqp-producer-reply-interval").getValue();
        int smqpConsumerCacheSize = (Integer) cfEntity.getProperty("smqp-consumer-cache-size").getValue();
        int smqpConsumerCacheSizeKB = (Integer) cfEntity.getProperty("smqp-consumer-cache-size-kb").getValue();
        String s = ((String) cfEntity.getProperty("jms-default-delivery-mode").getValue()).toLowerCase();
        int jmsDeliveryMode = s.equals("persistent") ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
        int jmsPriority = (Integer) cfEntity.getProperty("jms-default-message-priority").getValue();
        long jmsTTL = (Long) cfEntity.getProperty("jms-default-message-ttl").getValue();
        boolean jmsMessageIdEnabled = (Boolean) cfEntity.getProperty("jms-default-message-id-enabled").getValue();
        boolean jmsMessageTimestampEnabled = (Boolean) cfEntity.getProperty("jms-default-message-timestamp-enabled").getValue();
        boolean useThreadContextCL = (Boolean) cfEntity.getProperty("thread-context-classloader-for-getobject").getValue();


        com.swiftmq.jms.v750.ConnectionFactoryImpl cf7 = new com.swiftmq.jms.v750.ConnectionFactoryImpl(INTRAVM_LISTENER,
                null,
                null,
                0,
                0,
                clientId,
                smqpProducerReplyInterval,
                smqpConsumerCacheSize,
                smqpConsumerCacheSizeKB,
                jmsDeliveryMode,
                jmsPriority,
                jmsTTL,
                jmsMessageIdEnabled,
                jmsMessageTimestampEnabled,
                useThreadContextCL,
                0,
                0,
                0,
                0,
                true);
        DataByteArrayOutputStream dos7 = new DataByteArrayOutputStream();
        dos7.writeInt(cf7.getDumpId());
        cf7.writeContent(dos7);

        Versionable versionable = new Versionable();
        versionable.addVersioned(750, new Versioned(750, dos7.getBuffer(), dos7.getCount()), "com.swiftmq.jms.v750.CFFactory");
        ctx.jndiSwiftlet.registerJNDIObject(cfEntity.getName(), versionable);
    }

    private void createIVMConnectionFactories(EntityList cfList) throws Exception {
        Map map = cfList.getEntities();
        if (map != null) {
            for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); ) {
                createIVMConnectionFactory((Entity) ((Map.Entry) iter.next()).getValue());
            }
        }
        cfList.setEntityAddListener(new EntityChangeAdapter(null) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                try {
                    createIVMConnectionFactory(newEntity);
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "onEntityAdd (IVM connection factory): new cf=" + newEntity.getName());
                } catch (Exception e) {
                    throw new EntityAddException(e.toString());
                }
            }
        });
        cfList.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd (IVM connection factory): del cf=" + delEntity.getName());
                ctx.jndiSwiftlet.deregisterJNDIObject(delEntity.getName());
            }
        });
    }

    private static class CFComparable implements Comparable {
        String listenerName = null;

        CFComparable(String listenerName) {
            this.listenerName = listenerName;
        }

        public int compareTo(Object o) {
            if (o instanceof Versionable) {
                Versionable v = (Versionable) o;
                try {
                    com.swiftmq.jms.v750.ConnectionFactoryImpl cf = (com.swiftmq.jms.v750.ConnectionFactoryImpl) v.createCurrentVersionObject();
                    return cf.getListenerName().compareTo(listenerName);
                } catch (Exception ignored) {
                }
            }
            return -1;
        }
    }

    private void createHostAccessList(ListenerMetaData meta, EntityList haEntitiy) {
        Map h = haEntitiy.getEntities();
        if (!h.isEmpty()) {
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
        long keepAliveInterval = (Long) listenerEntity.getProperty("keepalive-interval").getValue();
        InetAddress bindAddress = null;
        try {
            String s = (String) listenerEntity.getProperty("bindaddress").getValue();
            if (s != null && !s.trim().isEmpty())
                bindAddress = InetAddress.getByName(s);
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }

        String connectIP = (String) listenerEntity.getProperty("connectaddress").getValue();
        if (connectIP != null && !connectIP.trim().isEmpty())
            connectAddresses.put(listenerName, connectIP);
        int inputBufferSize = ((Integer) listenerEntity.getProperty("router-input-buffer-size").getValue()).intValue();
        int inputExtendSize = ((Integer) listenerEntity.getProperty("router-input-extend-size").getValue()).intValue();
        int outputBufferSize = ((Integer) listenerEntity.getProperty("router-output-buffer-size").getValue()).intValue();
        int outputExtendSize = ((Integer) listenerEntity.getProperty("router-output-extend-size").getValue()).intValue();
        boolean useTCPNoDelay = ((Boolean) listenerEntity.getProperty("use-tcp-no-delay").getValue()).booleanValue();
        ListenerMetaData meta = new ListenerMetaData(bindAddress, port, this, keepAliveInterval, socketFactoryClass, new Acceptor(listenerName, listenerEntity.getProperty("max-connections")),
                inputBufferSize, inputExtendSize, outputBufferSize, outputExtendSize, useTCPNoDelay);
        listenerEntity.setUserObject(meta);
        createHostAccessList(meta, (EntityList) listenerEntity.getEntity("host-access-list"));
        EntityList cfList = (EntityList) listenerEntity.getEntity("connection-factories");
        try {
            createConnectionFactories(listenerEntity, cfList);
        } catch (Exception e) {
            throw new SwiftletException(e.toString());
        }

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "starting listener '" + listenerName + "' ...");
        try {
            ctx.networkSwiftlet.createTCPListener(meta);
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }
        if (cfList.getEntity(listenerName + "@" + SwiftletManager.getInstance().getRouterName()) == null) {
            ctx.timerSwiftlet.addInstantTimerListener(5000, new CFTimer(listenerName, cfList));
        }
        return meta;
    }

    private static class CFTimer implements TimerListener {
        String listenerName = null;
        EntityList cfList = null;

        public CFTimer(String listenerName, EntityList cfList) {
            this.listenerName = listenerName;
            this.cfList = cfList;
        }

        public void performTimeAction() {
            Entity entity = cfList.createEntity();
            entity.createCommands();
            entity.setName(listenerName + "@" + SwiftletManager.getInstance().getRouterName());
            try {
                cfList.addEntity(entity);
            } catch (EntityAddException e) {
                e.printStackTrace();
            }
        }
    }

    private void createListeners(EntityList listenerList) throws SwiftletException {
        String[] inboundNames = listenerList.getEntityNames();
        if (inboundNames != null) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "creating listeners ...");
            for (String listenerName : inboundNames) {
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
                String name = delEntity.getName();
                ctx.jndiSwiftlet.deregisterJNDIObjects(new CFComparable(name));
                ctx.networkSwiftlet.removeTCPListener((ListenerMetaData) delEntity.getUserObject());
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove (listener): listener=" + name);
            }
        });
    }

    private void createGeneralProps(Entity root) throws SwiftletException {
        Property prop = root.getProperty("crfactory-class");
        String crf = (String) root.getProperty("crfactory-class").getValue();
        try {
            challengeResponseFactory = (ChallengeResponseFactory) Class.forName(crf).newInstance();
        } catch (Exception e) {
            String msg = "Error creating class instance of challenge/response factory '" + crf + "', exception=" + e;
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), msg);
            throw new SwiftletException(msg);
        }
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                try {
                    ChallengeResponseFactory sf = (ChallengeResponseFactory) Class.forName((String) newValue).newInstance();
                } catch (Exception e) {
                    String msg = "Error creating class instance of default challenge/response factory '" + newValue + "', exception=" + e;
                    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), msg);
                    throw new PropertyChangeException(msg);
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "propertyChanged (crfactory.class): oldValue=" + oldValue + ", newValue=" + newValue);
            }
        });

        prop = root.getProperty("max-connections");
        maxConnections.set((Integer) prop.getValue());
        if (maxConnections.get() < -1 || maxConnections.get() == 0)
            throw new SwiftletException("Invalid Value, must be -1 (unlimited) or > 0");

        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                int n = ((Integer) newValue).intValue();
                if (n < -1 || n == 0)
                    throw new PropertyChangeException("Invalid Value, must be -1 (unlimited) or > 0");
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "propertyChanged (maxconnections): oldValue=" + oldValue + ", newValue=" + newValue);
                maxConnections.set(n);
            }
        });
    }

    public VersionedJMSConnection createJMSConnection(int version, Entity usage, Connection connection) {
        VersionedJMSConnection vc = null;
        switch (version) {
            case 750:
                vc = new com.swiftmq.impl.jms.standard.v750.JMSConnection(ctx, usage, connection);
                break;
        }
        return vc;
    }

    protected Semaphore getShutdownSemaphore() {
        if (!connections.isEmpty()) {
            shutdownSem.set(new Semaphore());
        } else {
            shutdownSem.set(null);
        }
        return shutdownSem.get();
    }

    protected void doConnect(Connection connection) throws ConnectionVetoException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "doConnect: " + connection);
        int currentMaxConnections = maxConnections.get();
        if (currentMaxConnections != -1) {
            // Additional check to reduce the window of race condition
            if (connections.size() >= currentMaxConnections) {
                throw new ConnectionVetoException("Maximum connections (" + currentMaxConnections + ") already reached!");
            }
            connections.add(connection);
            // Recheck the condition after adding the connection
            if (connections.size() > currentMaxConnections) {
                connections.remove(connection); // Revert the addition
                throw new ConnectionVetoException("Maximum connections (" + currentMaxConnections + ") exceeded after adding the connection!");
            }
        }
        Entity ce = ctx.usageList.createEntity();
        VersionSelector versionSelector = new VersionSelector(ctx, ce);
        connection.setInboundHandler(versionSelector);
        connection.setUserObject(versionSelector);
        ce.setName(connection.toString());
        ce.setDynamicObject(connection);
        ce.createCommands();
        try {
            ctx.usageList.addEntity(ce);
        } catch (Exception ignored) {
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "doConnect: " + connection + ", DONE.");
    }

    protected void doDisconnect(Connection connection) {
        // It may happen during shutdown that the Network Swiftlet calls this method and ctx becomes null
        SwiftletContext myCtx = ctx;
        if (myCtx == null)
            return;
        if (myCtx.traceSpace.enabled) myCtx.traceSpace.trace(getName(), "doDisconnect: " + connection);
        VersionSelector versionSelector = (VersionSelector) connection.getUserObject();
        if (versionSelector != null) {
            myCtx.usageList.removeDynamicEntity(connection);
            versionSelector.close();
            connections.remove(connection);
            if (shutdownSem.get() != null && connections.isEmpty())
                shutdownSem.get().notifySingleWaiter();
        }
        if (myCtx.traceSpace.enabled) myCtx.traceSpace.trace(getName(), "doDisconnect: " + connection + ", DONE.");
    }

    public void adminToolActivated() {
        collectOn.set(true);
        collectChanged(-1, collectInterval.get());
    }

    public void adminToolDeactivated() {
        collectChanged(collectInterval.get(), -1);
        collectOn.set(false);
    }

    protected void startup(Configuration config)
            throws SwiftletException {
        ctx = new SwiftletContext(config, this);

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup ...");
        Property prop = ctx.root.getProperty("allow-same-clientid");
        if (prop != null) {
            allowSameClientId.set((Boolean) prop.getValue());
            prop.setPropertyChangeListener(new PropertyChangeListener() {
                public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                    allowSameClientId.set((Boolean) newValue);
                }
            });
        }

        // set default props
        prop = ((EntityList) ctx.root.getEntity("listeners")).getTemplate().getProperty("socketfactory-class");
        prop.setDefaultProp(ctx.root.getProperty("socketfactory-class"));

        createGeneralProps(ctx.root);
        createListeners((EntityList) ctx.root.getEntity("listeners"));

        intraVMMetaData = new IntraVMListenerMetaData(this, new Acceptor("intravm", null));
        try {
            ctx.networkSwiftlet.createIntraVMListener(intraVMMetaData);
            createIVMConnectionFactories((EntityList) ctx.root.getEntity("intravm-connection-factories"));
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }
        prop = ctx.root.getProperty("collect-interval");
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
                    ctx.traceSpace.trace(getName(), "startup: registering thread count collector");
                ctx.timerSwiftlet.addTimerListener(collectInterval.get(), this);
            } else if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "startup: collect interval <= 0; no msg/s count collector");
        }

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "registering MgmtListener ...");
        ctx.mgmtSwiftlet.addMgmtListener(this);

        ctx.usageList.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                Connection myConnection = (Connection) delEntity.getDynamicObject();
                ConnectionManager connectionManager = ctx.networkSwiftlet.getConnectionManager();
                connectionManager.removeConnection(myConnection);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove (JMSConnection): " + myConnection);
            }
        });

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup: done");
    }

    protected void shutdown()
            throws SwiftletException {
        // true when shutdown while standby
        if (ctx == null)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown: stopping listener ...");

        ctx.jndiSwiftlet.deregisterJNDIObjects(new CFComparable(INTRAVM_LISTENER));
        ctx.networkSwiftlet.removeIntraVMListener(intraVMMetaData);

        EntityList listenerList = (EntityList) ctx.root.getEntity("listeners");
        String[] inboundNames = listenerList.getEntityNames();
        if (inboundNames != null) {
            for (String inboundName : inboundNames) {
                ListenerMetaData meta = (ListenerMetaData) listenerList.getEntity(inboundName).getUserObject();
                ctx.networkSwiftlet.removeTCPListener(meta);
            }
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown: shutdown all jms connections");
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
        clientSet.clear();
        ctx.mgmtSwiftlet.removeMgmtListener(this);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown: done");
        ctx = null;
    }

    private class Acceptor implements ConnectionListener {
        String name = null;
        final AtomicInteger localMax = new AtomicInteger(-1);
        final AtomicInteger currentCount = new AtomicInteger();

        Acceptor(String name, Property prop) {
            this.name = name;
            if (prop != null) {
                localMax.set((Integer) prop.getValue());
                prop.setPropertyChangeListener(new PropertyChangeListener() {
                    public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                        localMax.set((Integer) newValue);
                    }
                });
            }
        }

        public void connected(Connection connection) throws ConnectionVetoException {
            if (localMax.get() != -1 && currentCount.incrementAndGet() > localMax.get()) {
                currentCount.decrementAndGet();
                throw new ConnectionVetoException("Maximum connections (" + localMax.get() + ") for this listener '" + name + "' reached!");
            }
            doConnect(connection);
        }

        public void disconnected(Connection connection) {
            doDisconnect(connection);
            if (localMax.get() != -1) {
                currentCount.decrementAndGet();
            }
        }
    }
}

