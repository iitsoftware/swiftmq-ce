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

package com.swiftmq.extension.jmsbridge;

import com.swiftmq.extension.jmsbridge.accounting.AccountingProfile;
import com.swiftmq.extension.jmsbridge.accounting.JMSBridgeSourceFactory;
import com.swiftmq.extension.jmsbridge.jobs.JobRegistrar;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.Swiftlet;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.mgmt.event.MgmtListener;
import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.timer.event.TimerListener;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class JMSBridgeSwiftlet extends Swiftlet implements TimerListener {
    // Bridge-Threadpool names
    static final String TP_SVRMGR = "extension.xt$bridge.server.mgr";

    SwiftletContext ctx = null;
    JobRegistrar jobRegistrar = null;
    boolean collectOn = false;
    long collectInterval = -1;
    MgmtListener mgmtListener = null;
    Map accountingProfiles = new HashMap();
    JMSBridgeSourceFactory sourceFactory = null;

    Map servers = Collections.synchronizedMap(new HashMap());

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

    public AccountingProfile getAccountingProfile(String serverName, String bridgeName) {
        AccountingProfile ap = null;
        synchronized (accountingProfiles) {
            Map bridgeAPs = (Map) accountingProfiles.get(serverName);
            if (bridgeAPs != null)
                ap = (AccountingProfile) bridgeAPs.get(bridgeName);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "getAccountingProfile, serverName=" + serverName + ", bridgeName=" + bridgeName + ", returns=" + ap);
        return ap;
    }

    public void startAccounting(String serverName, String bridgeName, AccountingProfile accountingProfile) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "startAccounting, serverName=" + serverName + ", bridgeName=" + bridgeName + ", accountingfProfile=" + accountingProfile);
        ServerBridge server = (ServerBridge) servers.get(serverName);
        if (server != null) {
            DestinationBridge bridge = server.getBridging(bridgeName);
            if (bridge != null)
                bridge.startAccounting(accountingProfile);
        }
        synchronized (accountingProfiles) {
            Map bridgeAPs = (Map) accountingProfiles.get(serverName);
            if (bridgeAPs == null) {
                bridgeAPs = new HashMap();
                accountingProfiles.put(serverName, bridgeAPs);
            }
            bridgeAPs.put(bridgeName, accountingProfile);
        }
    }

    public void stopAccounting(String serverName, String bridgeName) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "stopAccounting, serverName=" + serverName + ", bridgeName=" + bridgeName);
        ServerBridge server = (ServerBridge) servers.get(serverName);
        if (server != null) {
            DestinationBridge bridge = server.getBridging(bridgeName);
            if (bridge != null)
                bridge.stopAccounting();
        }
        synchronized (accountingProfiles) {
            Map bridgeAPs = (Map) accountingProfiles.get(serverName);
            if (bridgeAPs != null)
                bridgeAPs.remove(bridgeName);
        }
    }

    public void flushAccounting(String serverName, String bridgeName) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "flushAccounting, serverName=" + serverName + ", bridgeName=" + bridgeName);
        ServerBridge server = (ServerBridge) servers.get(serverName);
        if (server != null) {
            DestinationBridge bridge = server.getBridging(bridgeName);
            if (bridge != null)
                bridge.flushAccounting();
        }
    }

    public synchronized void performTimeAction() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "performTimeAction ...");
        for (Iterator iter = servers.entrySet().iterator(); iter.hasNext(); ) {
            ServerBridge serverBridge = (ServerBridge) ((Map.Entry) iter.next()).getValue();
            serverBridge.collect();
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "performTimeAction done");
    }

    private synchronized void destroyServer(Entity serverEntity, boolean closeIt) {
        ServerBridge server = (ServerBridge) servers.get(serverEntity.getName());
        if (server != null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "destroy server '" + serverEntity.getName() + "'");
            ctx.logSwiftlet.logInformation(getName(), "destroy server '" + serverEntity.getName() + "'");
            server.destroy();
            if (closeIt)
                server.close();
        }
    }

    private synchronized void destroyServer(Entity serverEntity) {
        destroyServer(serverEntity, false);
    }

    private synchronized void destroyServers(EntityList serverList) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "destroy servers ...");
        String[] s = serverList.getEntityNames();
        if (s != null) {
            for (int i = 0; i < s.length; i++) {
                destroyServer(serverList.getEntity(s[i]), true);
            }
            servers.clear();
        } else if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "no servers defined");
    }

    private synchronized void createServer(Entity serverEntity) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "create server '" + serverEntity.getName() + "'");
        ctx.logSwiftlet.logInformation(getName(), "create server '" + serverEntity.getName() + "'");
        servers.put(serverEntity.getName(), new ServerBridge(ctx, serverEntity));
    }

    private void createServers(EntityList serverList) throws SwiftletException {
        /*${evaltimer}*/
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "create servers ...");
        String[] s = serverList.getEntityNames();
        if (s != null) {
            for (int i = 0; i < s.length; i++) {
                try {
                    createServer(serverList.getEntity(s[i]));
                } catch (Exception e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "Exception creating server '" + s[i] + "': " + e);
                    ctx.logSwiftlet.logError(getName(), "Exception creating server '" + s[i] + "': " + e);
                }
            }
        } else if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "no servers defined");

        serverList.setEntityAddListener(new EntityChangeAdapter(null) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                String name = newEntity.getName();
                try {
                    createServer(newEntity);
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "onEntityAdd (servers): server=" + name);
                } catch (Exception e) {
                    String msg = "onEntityAdd (servers): Exception creating server '" + name + "': " + e;
                    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), msg);
                    ctx.logSwiftlet.logError(getName(), msg);
                    throw new EntityAddException(msg);
                }
            }
        });
        serverList.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                String name = delEntity.getName();
                destroyServer(delEntity);
                ServerBridge server = (ServerBridge) servers.get(name);
                server.close();
                servers.remove(name);
                if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "onEntityRemove (servers): server=" + name);
            }
        });
    }

    void destroyServer(String name) {
        ctx.threadpoolSwiftlet.dispatchTask(new Disconnector(name));
    }

    /**
     * Startup the swiftlet. Check if all required properties are defined and all other
     * startup conditions are met. Do startup work (i. e. start working thread, get/open resources).
     * If any condition prevends from startup fire a SwiftletException.
     *
     * @param config
     * @throws SwiftletException to prevend from startup
     */
    protected void startup(Configuration config)
            throws SwiftletException {

        try {
            ctx = new SwiftletContext(config, this);
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup ...");
            /*${evalstartupmark}*/
            createServers((EntityList) config.getEntity("servers"));
            jobRegistrar = new JobRegistrar(ctx, (EntityList) config.getEntity("servers"));
            jobRegistrar.register();
            Property prop = ctx.root.getProperty("collect-interval");
            if (prop != null) {
                prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
                    public void propertyChanged(Property property, Object oldValue, Object newValue)
                            throws PropertyChangeException {
                        collectInterval = ((Long) newValue).longValue();
                        collectChanged(((Long) oldValue).longValue(), collectInterval);
                    }
                });
                collectInterval = ((Long) prop.getValue()).longValue();
            }
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "registering MgmtListener ...");
            mgmtListener = new MgmtListener() {
                public void adminToolActivated() {
                    collectOn = true;
                    collectChanged(-1, collectInterval);
                }

                public void adminToolDeactivated() {
                    collectChanged(collectInterval, -1);
                    collectOn = false;
                }
            };
            ctx.mgmtSwiftlet.addMgmtListener(mgmtListener);

            if (ctx.accountingSwiftlet != null) {
                sourceFactory = new JMSBridgeSourceFactory(ctx);
                ctx.accountingSwiftlet.addAccountingSourceFactory(sourceFactory.getGroup(), sourceFactory.getName(), sourceFactory);
            }
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup done.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Shutdown the swiftlet. Check if all shutdown conditions are met. Do shutdown work (i. e. stop working thread, close resources).
     * If any condition prevends from shutdown fire a SwiftletException.
     *
     * @throws SwiftletException to prevend from shutdown
     */
    protected void shutdown()
            throws SwiftletException {
        if (ctx == null)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown ...");
        jobRegistrar.unregister();
        Property prop = ctx.root.getProperty("collect-interval");
        if (prop != null)
            prop.setPropertyChangeListener(null);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "remove MgmtListener ...");
        if (mgmtListener != null)
            ctx.mgmtSwiftlet.removeMgmtListener(mgmtListener);
        destroyServers((EntityList) ctx.config.getEntity("servers"));
        if (ctx.accountingSwiftlet != null) {
            ctx.accountingSwiftlet.removeAccountingSourceFactory(sourceFactory.getGroup(), sourceFactory.getName());
            sourceFactory = null;
        }
        accountingProfiles.clear();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown done.");
    }

    private class Disconnector implements AsyncTask {
        String name = null;

        Disconnector(String name) {
            this.name = name;
        }

        public boolean isValid() {
            return true;
        }

        public String getDispatchToken() {
            return TP_SVRMGR;
        }

        public String getDescription() {
            return getName() + "/Disconnector for server: " + name;
        }

        public void stop() {
        }

        public void run() {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "destroy server '" + name + "'");
            destroyServer(ctx.config.getEntity("servers").getEntity(name));
        }
    }
}

