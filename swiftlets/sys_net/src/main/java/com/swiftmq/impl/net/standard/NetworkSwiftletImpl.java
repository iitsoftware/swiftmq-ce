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

package com.swiftmq.impl.net.standard;

import com.swiftmq.impl.net.standard.scheduler.*;
import com.swiftmq.mgmt.*;
import com.swiftmq.net.client.IntraVMConnection;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.event.SwiftletManagerAdapter;
import com.swiftmq.swiftlet.event.SwiftletManagerEvent;
import com.swiftmq.swiftlet.mgmt.MgmtSwiftlet;
import com.swiftmq.swiftlet.mgmt.event.MgmtListener;
import com.swiftmq.swiftlet.net.*;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;

public class NetworkSwiftletImpl extends NetworkSwiftlet
        implements TimerListener {
    public static final String TP_CONNMGR = "sys$net.connection.mgr";
    public static final String TP_CONNHANDLER = "sys$net.connection.handler";

    protected Configuration config = null;
    MgmtSwiftlet mgmtSwiftlet = null;
    TimerSwiftlet timerSwiftlet = null;
    TraceSwiftlet traceSwiftlet = null;
    TraceSpace traceSpace = null;
    protected IOScheduler ioScheduler = null;
    protected IntraVMScheduler ivmScheduler = null;
    boolean collectOn = false;
    long collectInterval = -1;
    boolean reuseServerSocket = true;
    boolean dnsResolve = true;
    boolean setSocketOptions = false;
    long zombiConnectionTimeout = 0;

    private void collectChanged(long oldInterval, long newInterval) {
        if (!collectOn)
            return;
        if (traceSpace.enabled)
            traceSpace.trace(getName(), "collectChanged: old interval: " + oldInterval + " new interval: " + newInterval);
        if (oldInterval > 0) {
            if (traceSpace.enabled)
                traceSpace.trace(getName(), "collectChanged: removeTimerListener for interval " + oldInterval);
            timerSwiftlet.removeTimerListener(this);
        }
        if (newInterval > 0) {
            if (traceSpace.enabled)
                traceSpace.trace(getName(), "collectChanged: addTimerListener for interval " + newInterval);
            timerSwiftlet.addTimerListener(newInterval, this);
        }
    }

    public void performTimeAction() {
        if (traceSpace.enabled) traceSpace.trace(getName(), "collecting byte counts...");
        ((ConnectionManagerImpl) getConnectionManager()).collectByteCounts();
        if (traceSpace.enabled) traceSpace.trace(getName(), "collecting byte counts...DONE.");
    }

    public boolean isReuseServerSocket() {
        return reuseServerSocket;
    }

    public boolean isDnsResolve() {
        return dnsResolve;
    }

    public boolean isSetSocketOptions() {
        return setSocketOptions;
    }

    public long getZombiConnectionTimeout() {
        return zombiConnectionTimeout;
    }

    public void createTCPListener(ListenerMetaData metaData)
            throws Exception {
        if (traceSpace.enabled) traceSpace.trace(getName(), "createTCPListener: MetaData=" + metaData);
        int id = ioScheduler.createListener(metaData);
        metaData.setId(id);
        TCPListener l = ioScheduler.getListener(id);
        l.start();
    }

    public void removeTCPListener(ListenerMetaData metaData) {
        if (traceSpace.enabled) traceSpace.trace(getName(), "removeTCPListener: MetaData=" + metaData);
        ioScheduler.removeListener(metaData.getId());
    }

    public void createIntraVMListener(IntraVMListenerMetaData metaData) throws Exception {
        if (traceSpace.enabled) traceSpace.trace(getName(), "createIntraVMListener: MetaData=" + metaData);
        ivmScheduler.createListener(metaData);
    }

    public void removeIntraVMListener(IntraVMListenerMetaData metaData) {
        if (traceSpace.enabled) traceSpace.trace(getName(), "removeIntraVMListener: MetaData=" + metaData);
        ivmScheduler.removeListener(metaData);
    }

    public void connectIntraVMListener(String swiftletName, IntraVMConnection connection) throws Exception {
        if (traceSpace.enabled) traceSpace.trace(getName(), "connectIntraVMListener: swiftletName=" + swiftletName);
        ivmScheduler.connectListener(swiftletName, connection);
    }

    public void createTCPConnector(ConnectorMetaData metaData)
            throws Exception {
        if (traceSpace.enabled) traceSpace.trace(getName(), "createTCPConnector: MetaData=" + metaData);
        int id = ioScheduler.createConnector(metaData);
        metaData.setId(id);
        TCPConnector c = ioScheduler.getConnector(id);
        c.start();
    }

    public void removeTCPConnector(ConnectorMetaData metaData) {
        if (traceSpace.enabled) traceSpace.trace(getName(), "removeTCPConnector: MetaData=" + metaData);
        ioScheduler.removeConnector(metaData.getId());
    }

    protected IOScheduler createIOScheduler() {
        return new BlockingIOScheduler();
    }

    protected void startup(Configuration config)
            throws SwiftletException {
        this.config = config;

        timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);

        if (traceSpace.enabled) traceSpace.trace(getName(), "startup ...");

        setConnectionManager(new ConnectionManagerImpl((EntityList) config.getEntity("usage")));

        ioScheduler = createIOScheduler();
        ivmScheduler = new IntraVMScheduler(this);

        Property prop = config.getProperty("reuse-serversockets");
        reuseServerSocket = ((Boolean) prop.getValue()).booleanValue();

        prop = config.getProperty("dns-resolve-enabled");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                dnsResolve = ((Boolean) newValue).booleanValue();
            }
        });
        dnsResolve = ((Boolean) prop.getValue()).booleanValue();

        prop = config.getProperty("set-socket-options");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                setSocketOptions = ((Boolean) newValue).booleanValue();
            }
        });
        setSocketOptions = ((Boolean) prop.getValue()).booleanValue();

        prop = config.getProperty("zombi-connection-timeout");
        zombiConnectionTimeout = ((Long) prop.getValue()).longValue();
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                zombiConnectionTimeout = ((Long) newValue).longValue();
            }
        });

        prop = config.getProperty("collect-interval");
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
                if (traceSpace.enabled) traceSpace.trace(getName(), "startup: registering byte count collector");
                timerSwiftlet.addTimerListener(collectInterval, this);
            } else if (traceSpace.enabled)
                traceSpace.trace(getName(), "startup: collect interval <= 0; no byte count collector");
        }
        try {
            SwiftletManager.getInstance().addSwiftletManagerListener("sys$mgmt", new SwiftletManagerAdapter() {
                public void swiftletStarted(SwiftletManagerEvent evt) {
                    try {
                        mgmtSwiftlet = (MgmtSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$mgmt");
                        if (traceSpace.enabled) traceSpace.trace(getName(), "registering MgmtListener ...");
                        mgmtSwiftlet.addMgmtListener(new MgmtListener() {
                            public void adminToolActivated() {
                                collectOn = true;
                                collectChanged(-1, collectInterval);
                            }

                            public void adminToolDeactivated() {
                                collectChanged(collectInterval, -1);
                                collectOn = false;
                            }
                        });
                    } catch (Exception e) {
                        if (traceSpace.enabled) traceSpace.trace(getName(), "swiftletStartet, exception=" + e);
                    }
                }
            });
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }
        if (traceSpace.enabled) traceSpace.trace(getName(), "startup done.");
    }

    protected void shutdown() throws SwiftletException {
        if (traceSpace.enabled) traceSpace.trace(getName(), "shutdown ...");
        ConnectionManager cm = getConnectionManager();
        int cnt = 0;
        while (cm.getNumberConnections() > 0 && cnt < 10) {
            if (traceSpace.enabled) traceSpace.trace(getName(), "shutdown: waiting for connection termination...");
            System.out.println("+++ waiting for connection termination ...");
            try {
                Thread.sleep(1000);
            } catch (Exception ignored) {
            }
            cnt++;
        }

        if (traceSpace.enabled) traceSpace.trace(getName(), "shutdown: removing all connections");
        cm.removeAllConnections();

        if (traceSpace.enabled) traceSpace.trace(getName(), "shutdown: closing IOScheduler");
        ioScheduler.close();
        ioScheduler = null;
        ivmScheduler.close();
        ivmScheduler = null;

        if (isReuseServerSocket() && SwiftletManager.getInstance().isRebooting()) {
            if (traceSpace.enabled) traceSpace.trace(getName(), "shutdown: waiting 60s for listener interruption...");
            System.out.println("+++ waiting 60s for listener interruption ...");
            try {
                Thread.sleep(60000);
            } catch (Exception ignored) {
            }
        }
        if (traceSpace.enabled) traceSpace.trace(getName(), "shutdown done.");
    }
}

