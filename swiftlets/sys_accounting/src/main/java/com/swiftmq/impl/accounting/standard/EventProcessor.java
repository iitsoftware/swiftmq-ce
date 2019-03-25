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

package com.swiftmq.impl.accounting.standard;

import com.swiftmq.impl.accounting.standard.po.*;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.accounting.*;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.PipelineQueue;

import java.util.*;

public class EventProcessor implements EventVisitor {
    static final String TP_ACCOUNTING = "sys$accounting.eventprocessor";

    static final String STATE_DISABLED = "Connection disabled";
    static final String STATE_WAIT_SOURCE = "Waiting for Source Factory";
    static final String STATE_WAIT_SINK = "Waiting for Sink Factory";
    static final String STATE_WAIT_SOURCE_SINK = "Waiting for Source & Sink Factory";
    static final String STATE_ACTIVE = "Connection active";
    static final String STATE_ERROR = "Connection error: ";

    SwiftletContext ctx = null;
    PipelineQueue pipelineQueue = null;
    boolean closed = false;
    EntityList groupList = null;
    EntityList connectionList = null;
    Set activeSingletons = new HashSet();

    public EventProcessor(SwiftletContext ctx) {
        this.ctx = ctx;
        groupList = (EntityList) ctx.usage.getEntity("groups");
        connectionList = (EntityList) ctx.usage.getEntity("active-connections");
        pipelineQueue = new PipelineQueue(ctx.threadpoolSwiftlet.getPool(TP_ACCOUNTING), TP_ACCOUNTING, this);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/created");
    }

    private void createParameters(EntityList entityParms, Map factoryParms) {
        for (Iterator iter = factoryParms.entrySet().iterator(); iter.hasNext(); ) {
            Parameter parm = (Parameter) ((Map.Entry) iter.next()).getValue();
            Entity pEntity = entityParms.createEntity();
            pEntity.setName(parm.getName());
            pEntity.createCommands();
            try {
                entityParms.addEntity(pEntity);
                Property prop = pEntity.getProperty("description");
                prop.setValue(parm.getDescription());
                prop = pEntity.getProperty("default");
                prop.setValue(parm.getDefaultValue());
                prop = pEntity.getProperty("mandatory");
                prop.setValue(new Boolean(parm.isMandatory()));
            } catch (Exception e) {
            }
        }
    }

    private void closeSourceFactoryConnections(String group, String name) {
        Map map = connectionList.getEntities();
        if (map != null && map.size() > 0) {
            for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); ) {
                Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
                String state = (String) entity.getProperty("state").getValue();
                String sourceGroup = (String) entity.getProperty("source-factory-group").getValue();
                String sourceName = (String) entity.getProperty("source-factory-name").getValue();
                if (sourceGroup.equals(group) && sourceName.equals(name)) {
                    String newState = null;
                    if (state.equals(STATE_ACTIVE)) {
                        AccountingPair pair = (AccountingPair) entity.getUserObject();
                        try {
                            if (pair.source != null)
                                pair.source.stopAccounting();
                            if (pair.sink != null)
                                pair.sink.close();
                        } catch (Exception e) {
                        }
                        activeSingletons.remove(pair.sourceFactory);
                        entity.setUserObject(null);
                        newState = STATE_WAIT_SOURCE;
                    } else if (state.equals(STATE_WAIT_SINK))
                        newState = STATE_WAIT_SOURCE_SINK;
                    try {
                        if (newState != null)
                            setState(entity.getProperty("state"), entity.getName(), newState, false);
                    } catch (Exception e) {
                    }
                }
            }
        }
    }

    private void closeSinkFactoryConnections(String group, String name) {
        Map map = connectionList.getEntities();
        if (map != null && map.size() > 0) {
            for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); ) {
                Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
                String state = (String) entity.getProperty("state").getValue();
                String sinkGroup = (String) entity.getProperty("sink-factory-group").getValue();
                String sinkName = (String) entity.getProperty("sink-factory-name").getValue();
                if (sinkGroup.equals(group) && sinkName.equals(name)) {
                    String newState = null;
                    if (state.equals(STATE_ACTIVE)) {
                        AccountingPair pair = (AccountingPair) entity.getUserObject();
                        try {
                            if (pair.source != null)
                                pair.source.stopAccounting();
                            if (pair.sink != null)
                                pair.sink.close();
                        } catch (Exception e) {
                        }
                        activeSingletons.remove(pair.sinkFactory);
                        entity.setUserObject(null);
                        newState = STATE_WAIT_SOURCE;
                    }
                    if (state.equals(STATE_WAIT_SOURCE))
                        newState = STATE_WAIT_SOURCE_SINK;
                    try {
                        if (newState != null)
                            setState(entity.getProperty("state"), entity.getName(), newState, false);
                    } catch (Exception e) {
                    }
                }
            }
        }
    }

    private Map getParameters(EntityList parmList, Map accParms) throws Exception {
        Map actParms = null;
        if (accParms != null) {
            for (Iterator iter = accParms.entrySet().iterator(); iter.hasNext(); ) {
                Parameter parm = (Parameter) ((Map.Entry) iter.next()).getValue();
                Entity entity = parmList.getEntity(parm.getName());
                String value = null;
                if (entity != null)
                    value = (String) entity.getProperty("value").getValue();
                if (value == null)
                    value = parm.getDefaultValue();
                if (value != null) {
                    ParameterVerifier verifier = parm.getVerifier();
                    if (verifier != null)
                        verifier.verify(parm, value);
                    if (actParms == null)
                        actParms = new HashMap();
                    actParms.put(parm.getName(), value);
                } else if (parm.isMandatory())
                    throw new Exception("Missing mandatory Parameter: " + parm.getName());
            }
            Map entities = parmList.getEntities();
            if (entities != null && entities.size() > 0) {
                StringBuffer b = new StringBuffer("Invalid parameters: ");
                boolean begin = true;
                boolean doThrow = false;
                for (Iterator iter = entities.keySet().iterator(); iter.hasNext(); ) {
                    String name = (String) iter.next();
                    if (accParms.get(name) == null) {
                        doThrow = true;
                        if (!begin) {
                            b.append(", ");
                        }
                        begin = false;
                        b.append(name);
                    }
                }
                if (doThrow)
                    throw new Exception(b.toString());
            }
        } else {
            Map entities = parmList.getEntities();
            if (entities != null && entities.size() > 0) {
                StringBuffer b = new StringBuffer("Invalid parameters: ");
                boolean begin = true;
                for (Iterator iter = entities.keySet().iterator(); iter.hasNext(); ) {
                    String name = (String) iter.next();
                    if (!begin) {
                        b.append(", ");
                    }
                    begin = false;
                    b.append(name);
                }
                throw new Exception(b.toString());
            }
        }
        return actParms;
    }

    private void startConnection(Entity activeConnection, AccountingSourceFactory sourceFactory, AccountingSinkFactory sinkFactory) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/startConnection, name=" + activeConnection.getName() + " ...");
        if (sourceFactory.isSingleton()) {
            if (activeSingletons.contains(sourceFactory))
                throw new Exception("Source factory allows only 1 running instance");
            else
                activeSingletons.add(sourceFactory);
        }
        if (sinkFactory.isSingleton()) {
            if (activeSingletons.contains(sinkFactory))
                throw new Exception("Sink factory allows only 1 running instance");
            else
                activeSingletons.add(sinkFactory);
        }
        AccountingPair pair = new AccountingPair();
        activeConnection.setUserObject(pair);
        pair.sourceFactory = sourceFactory;
        pair.source = sourceFactory.create(getParameters((EntityList) ctx.root.getEntity("connections").getEntity(activeConnection.getName()).getEntity("source-factory-parameters"), sourceFactory.getParameters()));
        pair.sinkFactory = sinkFactory;
        pair.sink = sinkFactory.create(getParameters((EntityList) ctx.root.getEntity("connections").getEntity(activeConnection.getName()).getEntity("sink-factory-parameters"), sinkFactory.getParameters()));
        pair.source.setStopListener(new StopListenerImpl(activeConnection));
        pair.source.startAccounting(pair.sink);
        ctx.logSwiftlet.logInformation(ctx.accountingSwiftlet.getName(), toString() + "/Accounting connection '" + activeConnection.getName() + "' started");
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/startConnection, name=" + activeConnection.getName() + " done.");
    }

    private void setState(Property prop, String name, String state, boolean error) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/setState, name=" + name + ", state=" + state);
        if (error)
            ctx.logSwiftlet.logError(ctx.accountingSwiftlet.getName(), toString() + "/setState, accounting connection, name=" + name + ", state=" + state);
        else
            ctx.logSwiftlet.logInformation(ctx.accountingSwiftlet.getName(), toString() + "/setState, accounting connection, name=" + name + ", state=" + state);
        try {
            prop.setValue(state);
        } catch (Exception e) {
        }
    }

    private void checkConnectionState(boolean enabled, Entity activeConnection, AccountingSourceFactory sourceFactory, AccountingSinkFactory sinkFactory)
            throws Exception {
        Property state = activeConnection.getProperty("state");
        if (enabled) {
            if (sourceFactory == null && sinkFactory == null)
                setState(state, activeConnection.getName(), STATE_WAIT_SOURCE_SINK, false);
            else if (sourceFactory == null)
                setState(state, activeConnection.getName(), STATE_WAIT_SOURCE, false);
            else if (sinkFactory == null)
                setState(state, activeConnection.getName(), STATE_WAIT_SINK, false);
            else {
                startConnection(activeConnection, sourceFactory, sinkFactory);
                setState(state, activeConnection.getName(), STATE_ACTIVE, false);
            }
        } else
            setState(state, activeConnection.getName(), STATE_DISABLED, false);
    }

    private AccountingSourceFactory getSourceFactory(String groupName, String factoryName) {
        AccountingSourceFactory factory = null;
        Entity group = groupList.getEntity(groupName);
        if (group != null) {
            EntityList sfList = (EntityList) group.getEntity("source-factories");
            Entity sf = sfList.getEntity(factoryName);
            if (sf != null)
                factory = (AccountingSourceFactory) sf.getUserObject();
        }
        return factory;
    }

    private AccountingSinkFactory getSinkFactory(String groupName, String factoryName) {
        AccountingSinkFactory factory = null;
        Entity group = groupList.getEntity(groupName);
        if (group != null) {
            EntityList sfList = (EntityList) group.getEntity("sink-factories");
            Entity sf = sfList.getEntity(factoryName);
            if (sf != null)
                factory = (AccountingSinkFactory) sf.getUserObject();
        }
        return factory;
    }

    public void enqueue(POObject po) {
        pipelineQueue.enqueue(po);
    }

    public void visit(SourceFactoryAdded po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/" + po + " ...");
        if (closed)
            return;
        Entity group = groupList.getEntity(po.getGroup());
        if (group == null) {
            group = groupList.createEntity();
            group.setName(po.getGroup());
            group.createCommands();
            try {
                groupList.addEntity(group);
            } catch (EntityAddException e) {
            }
        }
        EntityList sfList = (EntityList) group.getEntity("source-factories");
        Entity sf = sfList.getEntity(po.getName());
        if (sf == null) {
            sf = sfList.createEntity();
            sf.setName(po.getName());
            sf.createCommands();
            try {
                sfList.addEntity(sf);
            } catch (EntityAddException e) {
            }
            AccountingSourceFactory factory = po.getFactory();
            try {
                sf.getProperty("singleton").setValue(new Boolean(factory.isSingleton()));
            } catch (Exception e) {
            }
            Map parameters = factory.getParameters();
            if (parameters != null)
                createParameters((EntityList) sf.getEntity("parameters"), parameters);
            sf.setUserObject(factory);
            po.setSuccess(true);
        } else {
            po.setSuccess(false);
            po.setException("Source factory '" + po.getName() + "' already defined in group '" + po.getGroup());
        }
        enqueue(new CheckConnections());
        Semaphore sem = po.getSemaphore();
        if (sem != null)
            sem.notifySingleWaiter();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(SourceFactoryRemoved po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/" + po + " ...");
        if (closed)
            return;
        closeSourceFactoryConnections(po.getGroup(), po.getName());
        Entity group = groupList.getEntity(po.getGroup());
        if (group != null) {
            EntityList sfList = (EntityList) group.getEntity("source-factories");
            Entity sf = sfList.getEntity(po.getName());
            try {
                if (sf != null)
                    sfList.removeEntity(sf);
            } catch (EntityRemoveException e) {
            }
        }
        po.setSuccess(true);
        Semaphore sem = po.getSemaphore();
        if (sem != null)
            sem.notifySingleWaiter();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(SinkFactoryAdded po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/" + po + " ...");
        if (closed)
            return;
        Entity group = groupList.getEntity(po.getGroup());
        if (group == null) {
            group = groupList.createEntity();
            group.setName(po.getGroup());
            group.createCommands();
            try {
                groupList.addEntity(group);
            } catch (EntityAddException e) {
            }
        }
        EntityList sfList = (EntityList) group.getEntity("sink-factories");
        Entity sf = sfList.getEntity(po.getName());
        if (sf == null) {
            sf = sfList.createEntity();
            sf.setName(po.getName());
            sf.createCommands();
            try {
                sfList.addEntity(sf);
            } catch (EntityAddException e) {
            }
            AccountingSinkFactory factory = po.getFactory();
            try {
                sf.getProperty("singleton").setValue(new Boolean(factory.isSingleton()));
            } catch (Exception e) {
            }
            Map parameters = factory.getParameters();
            if (parameters != null)
                createParameters((EntityList) sf.getEntity("parameters"), parameters);
            sf.setUserObject(factory);
            po.setSuccess(true);
        } else {
            po.setSuccess(false);
            po.setException("Sink factory '" + po.getName() + "' already defined in group '" + po.getGroup());
        }
        enqueue(new CheckConnections());
        Semaphore sem = po.getSemaphore();
        if (sem != null)
            sem.notifySingleWaiter();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(SinkFactoryRemoved po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/" + po + " ...");
        if (closed)
            return;
        closeSinkFactoryConnections(po.getGroup(), po.getName());
        Entity group = groupList.getEntity(po.getGroup());
        if (group != null) {
            EntityList sfList = (EntityList) group.getEntity("sink-factories");
            Entity sf = sfList.getEntity(po.getName());
            try {
                if (sf != null)
                    sfList.removeEntity(sf);
            } catch (EntityRemoveException e) {
            }
        }
        po.setSuccess(true);
        Semaphore sem = po.getSemaphore();
        if (sem != null)
            sem.notifySingleWaiter();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(ConnectionAdded po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/" + po + " ...");
        if (closed)
            return;
        Entity entity = po.getEntity();
        boolean enabled = ((Boolean) entity.getProperty("enabled").getValue()).booleanValue();

        Entity activeConnection = connectionList.createEntity();
        activeConnection.setName(entity.getName());
        activeConnection.createCommands();
        try {
            connectionList.addEntity(activeConnection);
            String sourceGroupName = (String) entity.getProperty("source-factory-group").getValue();
            String sourceFactoryName = (String) entity.getProperty("source-factory-name").getValue();
            String sinkGroupName = (String) entity.getProperty("sink-factory-group").getValue();
            String sinkFactoryName = (String) entity.getProperty("sink-factory-name").getValue();

            activeConnection.getProperty("source-factory-group").setValue(sourceGroupName);
            activeConnection.getProperty("source-factory-name").setValue(sourceFactoryName);
            AccountingSourceFactory sourceFactory = getSourceFactory(sourceGroupName, sourceFactoryName);

            activeConnection.getProperty("sink-factory-group").setValue(sinkGroupName);
            activeConnection.getProperty("sink-factory-name").setValue(sinkFactoryName);
            AccountingSinkFactory sinkFactory = getSinkFactory(sinkGroupName, sinkFactoryName);

            checkConnectionState(enabled, activeConnection, sourceFactory, sinkFactory);
            po.setSuccess(true);
        } catch (Exception e) {
            ctx.logSwiftlet.logError(ctx.accountingSwiftlet.getName(), toString() + "/" + e.getMessage());
            po.setSuccess(false);
            po.setException(e.toString());
        }
        Semaphore sem = po.getSemaphore();
        if (sem != null)
            sem.notifySingleWaiter();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(ConnectionRemoved po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/" + po + " ...");
        if (closed)
            return;
        po.setSuccess(true);
        Entity activeConnection = connectionList.getEntity(po.getName());
        String state = (String) activeConnection.getProperty("state").getValue();
        if (state.equals(STATE_ACTIVE)) {
            AccountingPair pair = (AccountingPair) activeConnection.getUserObject();
            try {
                if (pair.source != null)
                    pair.source.stopAccounting();
                if (pair.sink != null)
                    pair.sink.close();
            } catch (Exception e) {
            }
            activeConnection.setUserObject(null);
        }
        try {
            connectionList.removeEntity(activeConnection);
        } catch (EntityRemoveException e) {
        }
        Semaphore sem = po.getSemaphore();
        if (sem != null)
            sem.notifySingleWaiter();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(ConnectionStateChanged po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/" + po + " ...");
        if (closed)
            return;
        Entity entity = po.getEntity();
        Entity activeConnection = connectionList.getEntity(entity.getName());
        po.setSuccess(true);
        if (po.isEnabled()) {
            AccountingSourceFactory sourceFactory = null;
            AccountingSinkFactory sinkFactory = null;
            try {
                String sourceGroupName = (String) entity.getProperty("source-factory-group").getValue();
                String sourceFactoryName = (String) entity.getProperty("source-factory-name").getValue();
                String sinkGroupName = (String) entity.getProperty("sink-factory-group").getValue();
                String sinkFactoryName = (String) entity.getProperty("sink-factory-name").getValue();

                activeConnection.getProperty("source-factory-group").setValue(sourceGroupName);
                activeConnection.getProperty("source-factory-name").setValue(sourceFactoryName);
                sourceFactory = getSourceFactory(sourceGroupName, sourceFactoryName);

                activeConnection.getProperty("sink-factory-group").setValue(sinkGroupName);
                activeConnection.getProperty("sink-factory-name").setValue(sinkFactoryName);
                sinkFactory = getSinkFactory(sinkGroupName, sinkFactoryName);

                checkConnectionState(po.isEnabled(), activeConnection, sourceFactory, sinkFactory);
            } catch (Exception e) {
                ctx.logSwiftlet.logError(ctx.accountingSwiftlet.getName(), toString() + "/" + e.getMessage());
                if (sourceFactory != null)
                    activeSingletons.remove(sourceFactory);
                if (sinkFactory != null)
                    activeSingletons.remove(sinkFactory);
                po.setSuccess(false);
                po.setException(e.getMessage());
            }
        } else {
            AccountingPair pair = (AccountingPair) activeConnection.getUserObject();
            if (pair != null) {
                try {
                    if (pair.source != null)
                        pair.source.stopAccounting();
                    if (pair.sink != null)
                        pair.sink.close();
                } catch (Exception e) {
                }
                if (pair.sourceFactory != null)
                    activeSingletons.remove(pair.sourceFactory);
                if (pair.sinkFactory != null)
                    activeSingletons.remove(pair.sinkFactory);
                activeConnection.setUserObject(null);
                try {
                    activeConnection.getProperty("state").setValue(STATE_DISABLED);
                } catch (Exception e) {
                }
            }
        }

        Semaphore sem = po.getSemaphore();
        if (sem != null)
            sem.notifySingleWaiter();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(CheckConnections po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/" + po + " ...");
        if (closed)
            return;
        Map map = connectionList.getEntities();
        if (map != null && map.size() > 0) {
            for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); ) {
                Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
                String state = (String) entity.getProperty("state").getValue();
                if (state.equals(STATE_WAIT_SOURCE) || state.equals(STATE_WAIT_SINK) || state.equals(STATE_WAIT_SOURCE_SINK)) {
                    String sourceGroup = (String) entity.getProperty("source-factory-group").getValue();
                    String sourceName = (String) entity.getProperty("source-factory-name").getValue();
                    AccountingSourceFactory sourceFactory = getSourceFactory(sourceGroup, sourceName);
                    String sinkGroup = (String) entity.getProperty("sink-factory-group").getValue();
                    String sinkName = (String) entity.getProperty("sink-factory-name").getValue();
                    AccountingSinkFactory sinkFactory = getSinkFactory(sinkGroup, sinkName);
                    try {
                        checkConnectionState(true, entity, sourceFactory, sinkFactory);
                    } catch (Exception e) {
                        ctx.logSwiftlet.logError(ctx.accountingSwiftlet.getName(), toString() + "/" + e.getMessage());
                        try {
                            entity.getProperty("state").setValue(STATE_ERROR + e.toString());
                        } catch (Exception e1) {
                        }
                    }
                }
            }
            po.setSuccess(true);
            Semaphore sem = po.getSemaphore();
            if (sem != null)
                sem.notifySingleWaiter();
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(ConnectionStopped po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/" + po + " ...");
        if (closed)
            return;
        Entity activeConnection = po.getActiveConnection();
        AccountingPair pair = (AccountingPair) activeConnection.getUserObject();
        try {
            if (pair.source != null)
                pair.source.stopAccounting();
            if (pair.sink != null)
                pair.sink.close();
        } catch (Exception e) {
        }
        activeSingletons.remove(pair.sourceFactory);
        activeSingletons.remove(pair.sinkFactory);
        activeConnection.setUserObject(null);
        try {
            setState(activeConnection.getProperty("state"), activeConnection.getName(), STATE_ERROR + po.getReason(), true);
        } catch (Exception e) {
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(Close po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/" + po + " ...");
        Map map = connectionList.getEntities();
        if (map != null && map.size() > 0) {
            for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); ) {
                Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
                String state = (String) entity.getProperty("state").getValue();
                if (state.equals(STATE_ACTIVE)) {
                    AccountingPair pair = (AccountingPair) entity.getUserObject();
                    try {
                        if (pair.source != null)
                            pair.source.stopAccounting();
                        if (pair.sink != null)
                            pair.sink.close();
                    } catch (Exception e) {
                    }
                    entity.setUserObject(null);
                }
                iter.remove();
            }
        }
        closed = true;
        po.setSuccess(true);
        Semaphore sem = po.getSemaphore();
        if (sem != null)
            sem.notifySingleWaiter();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/close ...");
        Semaphore sem = new Semaphore();
        pipelineQueue.enqueue(new Close(sem));
        sem.waitHere();
        pipelineQueue.close();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/close done");
    }

    public String toString() {
        return "EventProcessor";
    }

    private class AccountingPair {
        AccountingSourceFactory sourceFactory = null;
        AccountingSource source = null;
        AccountingSinkFactory sinkFactory = null;
        AccountingSink sink = null;
    }

    private class StopListenerImpl implements StopListener {
        Entity activeConnection = null;

        private StopListenerImpl(Entity activeConnection) {
            this.activeConnection = activeConnection;
        }

        public void sourceStopped(AccountingSource source, Exception e) {
            enqueue(new ConnectionStopped(null, activeConnection, e == null ? "No exception given" : e.getMessage()));
        }
    }
}
