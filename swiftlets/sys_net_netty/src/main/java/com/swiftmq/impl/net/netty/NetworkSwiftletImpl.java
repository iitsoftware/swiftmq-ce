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

package com.swiftmq.impl.net.netty;

import com.swiftmq.impl.net.netty.scheduler.*;
import com.swiftmq.mgmt.Configuration;
import com.swiftmq.mgmt.Property;
import com.swiftmq.mgmt.PropertyChangeAdapter;
import com.swiftmq.mgmt.PropertyChangeException;
import com.swiftmq.net.client.IntraVMConnection;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.event.SwiftletManagerAdapter;
import com.swiftmq.swiftlet.event.SwiftletManagerEvent;
import com.swiftmq.swiftlet.mgmt.MgmtSwiftlet;
import com.swiftmq.swiftlet.mgmt.event.MgmtListener;
import com.swiftmq.swiftlet.net.*;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.collection.ConcurrentList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class NetworkSwiftletImpl extends NetworkSwiftlet implements TimerListener {
    SwiftletContext ctx = null;
    protected IntraVMScheduler ivmScheduler = null;
    protected IOScheduler ioScheduler = null;
    final AtomicBoolean collectOn = new AtomicBoolean(false);
    final AtomicLong collectInterval = new AtomicLong(-1);
    boolean reuseServerSocket = true;
    boolean dnsResolve = true;
    long zombiConnectionTimeout = 0;
    final AtomicBoolean delayed = new AtomicBoolean(true);
    List<Integer> listenersToStart = new ConcurrentList<>(new ArrayList<>());
    List<Integer> connectorsToStart = new ConcurrentList<>(new ArrayList<>());

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
        ((ConnectionManagerImpl) getConnectionManager()).collectByteCounts();
    }

    @Override
    public boolean isReuseServerSocket() {
        return reuseServerSocket;
    }

    @Override
    public boolean isDnsResolve() {
        return dnsResolve;
    }

    public long getZombiConnectionTimeout() {
        return zombiConnectionTimeout;
    }

    @Override
    public void createTCPListener(ListenerMetaData metaData) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createTCPListener: MetaData=" + metaData);
        int id = ioScheduler.createListener(metaData);
        metaData.setId(id);
        if (!delayed.get() || metaData.getSwiftlet().getName().equals("sys$hacontroller")) {
            TCPListener l = ioScheduler.getListener(id);
            l.start();
        } else
            listenersToStart.add(id);
    }

    @Override
    public void removeTCPListener(ListenerMetaData metaData) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "removeTCPListener: MetaData=" + metaData);
        ioScheduler.removeListener(metaData.getId());
    }

    @Override
    public void createIntraVMListener(IntraVMListenerMetaData metaData) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createIntraVMListener: MetaData=" + metaData);
        ivmScheduler.createListener(metaData);
    }

    @Override
    public void removeIntraVMListener(IntraVMListenerMetaData metaData) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "removeIntraVMListener: MetaData=" + metaData);
        ivmScheduler.removeListener(metaData);
    }

    @Override
    public void connectIntraVMListener(String swiftletName, IntraVMConnection connection) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "connectIntraVMListener: swiftletName=" + swiftletName);
        ivmScheduler.connectListener(swiftletName, connection);
    }

    @Override
    public void createTCPConnector(ConnectorMetaData metaData) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createTCPConnector: MetaData=" + metaData);
        int id = ioScheduler.createConnector(metaData);
        metaData.setId(id);
        if (!delayed.get() || metaData.getSwiftlet().getName().equals("sys$hacontroller")) {
            TCPConnector c = ioScheduler.getConnector(id);
            c.start();
        } else
            connectorsToStart.add(id);
    }

    @Override
    public void removeTCPConnector(ConnectorMetaData metaData) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "removeTCPConnector: MetaData=" + metaData);
        ioScheduler.removeConnector(metaData.getId());
    }

    private void startListenerAndConnectors() throws Exception {
        for (int id : listenersToStart) {
            TCPListener listener = ioScheduler.getListener(id);
            ctx.logSwiftlet.logInformation(getName(), "Starting listener on port " + listener.getMetaData().getPort() + " [" + listener.getMetaData().getSwiftlet().getName() + "]");
            listener.start();
        }
        listenersToStart.clear();
        for (int id : connectorsToStart) {
            TCPConnector connector = ioScheduler.getConnector(id);
            ctx.logSwiftlet.logInformation(getName(), "Starting connector to " + connector.getMetaData().getHostname() + ":" + connector.getMetaData().getPort() + " [" + connector.getMetaData().getSwiftlet().getName() + "]");
            connector.start();
        }
        connectorsToStart.clear();
        delayed.set(false);
    }

    @Override
    public void setDelayed(boolean delayed) {
        this.delayed.set(delayed);
    }

    @Override
    public void startDelayed() throws Exception {
        if (delayed.get())
            startListenerAndConnectors();
    }

    @Override
    protected void startup(Configuration config) throws SwiftletException {
        try {
            ctx = new SwiftletContext(config, this);
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup ...");

        setConnectionManager(new ConnectionManagerImpl(ctx));
        ivmScheduler = new IntraVMScheduler(ctx);
        ioScheduler = new NettyIOScheduler(ctx);
        Property prop = config.getProperty("reuse-serversockets");
        reuseServerSocket = (Boolean) prop.getValue();

        prop = config.getProperty("dns-resolve-enabled");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                dnsResolve = (Boolean) newValue;
            }
        });
        dnsResolve = (Boolean) prop.getValue();

        prop = config.getProperty("zombi-connection-timeout");
        zombiConnectionTimeout = (Long) prop.getValue();
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                zombiConnectionTimeout = (Long) newValue;
            }
        });

        prop = config.getProperty("collect-interval");
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
                    ctx.traceSpace.trace(getName(), "startup: registering byte count collector");
                ctx.timerSwiftlet.addTimerListener(collectInterval.get(), this);
            } else if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "startup: collect interval <= 0; no byte count collector");
        }
        try {
            SwiftletManager.getInstance().addSwiftletManagerListener("sys$mgmt", new SwiftletManagerAdapter() {
                public void swiftletStarted(SwiftletManagerEvent evt) {
                    try {
                        ctx.mgmtSwiftlet = (MgmtSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$mgmt");
                        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "registering MgmtListener ...");
                        ctx. mgmtSwiftlet.addMgmtListener(new MgmtListener() {
                            public void adminToolActivated() {
                                collectOn.set(true);
                                collectChanged(-1, collectInterval.get());
                            }

                            public void adminToolDeactivated() {
                                collectChanged(collectInterval.get(), -1);
                                collectOn.set(false);
                            }
                        });
                    } catch (Exception e) {
                        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "swiftletStartet, exception=" + e);
                    }
                }
            });
            if (!SwiftletManager.getInstance().isHA()) {
                SwiftletManager.getInstance().addSwiftletManagerListener(SwiftletManager.getInstance().getLastSwiftlet(), new SwiftletManagerAdapter() {
                    @Override
                    public void swiftletStarted(SwiftletManagerEvent evt) {
                        try {
                            startListenerAndConnectors();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup DONE");

    }

    @Override
    protected void shutdown() throws SwiftletException {
        delayed.set(true);
        // true when shutdown while standby
        if (ctx == null)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown ...");
        ConnectionManager cm = getConnectionManager();
        int cnt = 0;
        while (cm.getNumberConnections() > 0 && cnt < 10) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown: waiting for connection termination...");
            System.out.println("+++ waiting for connection termination ...");
            try {
                Thread.sleep(1000);
            } catch (Exception ignored) {
            }
            cnt++;
        }

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown: removing all connections");
        cm.removeAllConnections();

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown: closing IOScheduler");
        ivmScheduler.close();
        ivmScheduler = null;
        ioScheduler.close();
        ioScheduler = null;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown DONE");
        ctx = null;
    }
}
