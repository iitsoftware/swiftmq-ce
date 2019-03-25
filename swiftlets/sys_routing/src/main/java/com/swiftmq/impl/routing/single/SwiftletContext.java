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

import com.swiftmq.auth.ChallengeResponseFactory;
import com.swiftmq.impl.routing.single.manager.ConnectionManager;
import com.swiftmq.impl.routing.single.route.RouteExchanger;
import com.swiftmq.impl.routing.single.schedule.SchedulerRegistry;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.Property;
import com.swiftmq.mgmt.PropertyChangeAdapter;
import com.swiftmq.mgmt.PropertyChangeException;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.accounting.AccountingSwiftlet;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.net.NetworkSwiftlet;
import com.swiftmq.swiftlet.queue.QueueManager;
import com.swiftmq.swiftlet.scheduler.SchedulerSwiftlet;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;
import com.swiftmq.swiftlet.xa.XAResourceManagerSwiftlet;

public class SwiftletContext {
    public TimerSwiftlet timerSwiftlet = null;
    public TraceSwiftlet traceSwiftlet = null;
    public TraceSpace traceSpace = null;
    public LogSwiftlet logSwiftlet = null;
    public ThreadpoolSwiftlet threadpoolSwiftlet = null;
    public NetworkSwiftlet networkSwiftlet = null;
    public QueueManager queueManager = null;
    public XAResourceManagerSwiftlet xaResourceManagerSwiftlet = null;
    public RoutingSwiftletImpl routingSwiftlet = null;
    public SchedulerSwiftlet schedulerSwiftlet = null;
    public AccountingSwiftlet accountingSwiftlet = null;
    public ConnectionManager connectionManager = null;
    public Entity root = null;
    public Entity usageList = null;
    public String routerName = null;
    public String unroutableQueue = null;
    public ChallengeResponseFactory challengeResponseFactory = null;
    public SchedulerRegistry schedulerRegistry = null;
    public boolean roundRobinEnabled = true;
    public boolean inboundFCEnabled = true;
    public RouteExchanger routeExchanger = null;

    protected SwiftletContext(RoutingSwiftletImpl routingSwiftlet, Entity root) throws SwiftletException {
        this.routingSwiftlet = routingSwiftlet;
        this.root = root;
        usageList = root.getEntity("usage");
        timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
        networkSwiftlet = (NetworkSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$net");
        threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
        accountingSwiftlet = (AccountingSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$accounting");
        queueManager = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
        xaResourceManagerSwiftlet = (XAResourceManagerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$xa");
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
        routerName = SwiftletManager.getInstance().getRouterName();
        unroutableQueue = RoutingSwiftletImpl.UNROUTABLE_QUEUE + '@' + routerName;
        routeExchanger = new RouteExchanger(this);
        connectionManager = createConnectionManager();
        connectionManager.addConnectionListener(routeExchanger);
        schedulerRegistry = new SchedulerRegistry(this);

        Property prop = root.getProperty("roundrobin-enabled");
        roundRobinEnabled = ((Boolean) prop.getValue()).booleanValue();

        prop = root.getProperty("crfactory-class");
        String crf = (String) root.getProperty("crfactory-class").getValue();
        try {
            challengeResponseFactory = (ChallengeResponseFactory) Class.forName(crf).newInstance();
        } catch (Exception e) {
            String msg = "Error creating class instance of challenge/response factory '" + crf + "', exception=" + e;
            if (traceSpace.enabled) traceSpace.trace(SwiftletContext.this.routingSwiftlet.getName(), msg);
            throw new SwiftletException(msg);
        }
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                try {
                    ChallengeResponseFactory sf = (ChallengeResponseFactory) Class.forName((String) newValue).newInstance();
                } catch (Exception e) {
                    String msg = "Error creating class instance of default challenge/response factory '" + newValue + "', exception=" + e;
                    if (traceSpace.enabled) traceSpace.trace(SwiftletContext.this.routingSwiftlet.getName(), msg);
                    throw new PropertyChangeException(msg);
                }
                if (traceSpace.enabled)
                    traceSpace.trace(SwiftletContext.this.routingSwiftlet.getName(), "propertyChanged (crfactory.class): oldValue=" + oldValue + ", newValue=" + newValue);
            }
        });

        prop = root.getProperty("inbound-flow-control-enabled");
        inboundFCEnabled = ((Boolean) prop.getValue()).booleanValue();

    }

    protected ConnectionManager createConnectionManager() {
        return new ConnectionManager(this);
    }
}
