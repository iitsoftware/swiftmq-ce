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

package com.swiftmq.impl.mgmt.standard;

import com.swiftmq.auth.ChallengeResponseFactory;
import com.swiftmq.impl.mgmt.standard.jmx.EntityMBean;
import com.swiftmq.impl.mgmt.standard.jmx.JMXUtil;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.event.KernelStartupListener;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.queue.QueueManager;
import com.swiftmq.swiftlet.routing.RoutingSwiftlet;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;

public class SwiftletContext implements KernelStartupListener {
    public static final String MGMT_QUEUE = "swiftmqmgmt";

    public MgmtSwiftletImpl mgmtSwiftlet = null;
    public TimerSwiftlet timerSwiftlet = null;
    public TraceSwiftlet traceSwiftlet = null;
    public TraceSpace traceSpace = null;
    public LogSwiftlet logSwiftlet = null;
    public ThreadpoolSwiftlet threadpoolSwiftlet = null;
    public RoutingSwiftlet routingSwiftlet = null;
    public QueueManager queueManager = null;
    public DispatchQueue dispatchQueue = null;
    public Entity root = null;
    public Entity usageList = null;
    public ChallengeResponseFactory challengeResponseFactory = null;
    public boolean authEnabled = false;
    public boolean connectLoggingEnabled = false;
    public String password = null;
    String name = null;
    public JMXUtil jmxUtil = null;
    public EntityMBean rootMBean = null;

    public SwiftletContext(MgmtSwiftletImpl mgmtSwiftlet, Entity root) throws SwiftletException {
        this.mgmtSwiftlet = mgmtSwiftlet;
        this.root = root;
        name = mgmtSwiftlet.getName();
        usageList = root.getEntity("usage");
        timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
        threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
        queueManager = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);

        dispatchQueue = new DispatchQueue(this);

        Property prop = root.getProperty("authentication-enabled");
        authEnabled = ((Boolean) prop.getValue()).booleanValue();
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                authEnabled = ((Boolean) newValue).booleanValue();
                if (traceSpace.enabled)
                    traceSpace.trace(name, "propertyChanged (authentication-enabled): oldValue=" + oldValue + ", newValue=" + newValue);
            }
        });

        prop = root.getProperty("admintool-connect-logging-enabled");
        connectLoggingEnabled = ((Boolean) prop.getValue()).booleanValue();
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                connectLoggingEnabled = ((Boolean) newValue).booleanValue();
                if (traceSpace.enabled)
                    traceSpace.trace(name, "propertyChanged (admintool-connect-logging-enabled): oldValue=" + oldValue + ", newValue=" + newValue);
            }
        });

        prop = root.getProperty("crfactory-class");
        String crf = (String) root.getProperty("crfactory-class").getValue();
        try {
            challengeResponseFactory = (ChallengeResponseFactory) Class.forName(crf).newInstance();
        } catch (Exception e) {
            String msg = "Error creating class instance of challenge/response factory '" + crf + "', exception=" + e;
            if (traceSpace.enabled) traceSpace.trace(name, msg);
            throw new SwiftletException(msg);
        }
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                try {
                    ChallengeResponseFactory sf = (ChallengeResponseFactory) Class.forName((String) newValue).newInstance();
                } catch (Exception e) {
                    String msg = "Error creating class instance of default challenge/response factory '" + newValue + "', exception=" + e;
                    if (traceSpace.enabled) traceSpace.trace(name, msg);
                    throw new PropertyChangeException(msg);
                }
                if (traceSpace.enabled)
                    traceSpace.trace(name, "propertyChanged (crfactory.class): oldValue=" + oldValue + ", newValue=" + newValue);
            }
        });
        prop = root.getProperty("password");
        password = (String) prop.getValue();
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                password = (String) newValue;
                if (traceSpace.enabled)
                    traceSpace.trace(name, "propertyChanged (password): oldValue=" + oldValue + ", newValue=" + newValue);
            }
        });
        try {
            if (traceSpace.enabled) traceSpace.trace(name, "checking whether queue " + MGMT_QUEUE + " exists ...");
            if (!queueManager.isQueueDefined(MGMT_QUEUE))
                queueManager.createQueue(MGMT_QUEUE, (ActiveLogin) null);
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }
        SwiftletManager.getInstance().addKernelStartupListener(this);
    }

    public void kernelStarted() {
        Property prop = root.getEntity("jmx").getProperty("enabled");
        if (((Boolean) prop.getValue()).booleanValue()) {
            jmxUtil = new JMXUtil(this, ((Boolean) root.getEntity("jmx").getProperty("groupable-objectnames").getValue()).booleanValue());
            mgmtSwiftlet.fireEvent(true);
            rootMBean = new EntityMBean(this, RouterConfiguration.Singleton());
        }
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                if (((Boolean) oldValue).booleanValue()) {
                    if (rootMBean != null) {
                        rootMBean.close();
                        rootMBean = null;
                        SwiftletContext.this.mgmtSwiftlet.fireEvent(false);
                    }
                }
                if (((Boolean) newValue).booleanValue()) {
                    if (jmxUtil == null)
                        jmxUtil = new JMXUtil(SwiftletContext.this, ((Boolean) root.getEntity("jmx").getProperty("groupable-objectnames").getValue()).booleanValue());
                    else
                        jmxUtil.setGroupNames(((Boolean) root.getEntity("jmx").getProperty("groupable-objectnames").getValue()).booleanValue());
                    SwiftletContext.this.mgmtSwiftlet.fireEvent(true);
                    rootMBean = new EntityMBean(SwiftletContext.this, RouterConfiguration.Singleton());
                }
                if (traceSpace.enabled)
                    traceSpace.trace(name, "propertyChanged (JMX enabled): oldValue=" + oldValue + ", newValue=" + newValue);
            }
        });
        prop = root.getEntity("jmx").getProperty("groupable-objectnames");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                if (rootMBean != null) {
                    rootMBean.close();
                    rootMBean = null;
                    jmxUtil.setGroupNames(((Boolean) newValue).booleanValue());
                    rootMBean = new EntityMBean(SwiftletContext.this, RouterConfiguration.Singleton());
                }
                if (traceSpace.enabled)
                    traceSpace.trace(name, "propertyChanged (JMX groupable-objectnames): oldValue=" + oldValue + ", newValue=" + newValue);
            }
        });

    }

    public void close() {
        if (rootMBean != null)
            rootMBean.close();
        dispatchQueue.close();
    }
}
