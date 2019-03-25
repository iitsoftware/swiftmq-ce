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

package com.swiftmq.impl.amqp;

import com.swiftmq.impl.amqp.amqp.v00_09_01.ExchangeRegistry;
import com.swiftmq.impl.amqp.amqp.v00_09_01.QueueMapper;
import com.swiftmq.impl.amqp.amqp.v01_00_00.transformer.TransformerFactory;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.accounting.AccountingSwiftlet;
import com.swiftmq.swiftlet.auth.AuthenticationSwiftlet;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.mgmt.MgmtSwiftlet;
import com.swiftmq.swiftlet.net.NetworkSwiftlet;
import com.swiftmq.swiftlet.queue.QueueManager;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.topic.TopicManager;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;

import java.util.HashSet;
import java.util.Set;

public class SwiftletContext {
    public Configuration config = null;
    public Entity root = null;
    public EntityList usageList = null;
    public AccountingSwiftlet accountingSwiftlet = null;
    public AuthenticationSwiftlet authSwiftlet = null;
    public ThreadpoolSwiftlet threadpoolSwiftlet = null;
    public TimerSwiftlet timerSwiftlet = null;
    public QueueManager queueManager = null;
    public TopicManager topicManager = null;
    public LogSwiftlet logSwiftlet = null;
    public MgmtSwiftlet mgmtSwiftlet = null;
    public NetworkSwiftlet networkSwiftlet = null;
    public TraceSwiftlet traceSwiftlet = null;
    public TraceSpace traceSpace = null;
    public TraceSpace protSpace = null;
    public AMQPSwiftlet amqpSwiftlet = null;
    public TransformerFactory transformerFactory = null;
    private Set connectedIds = new HashSet();
    private boolean allowSameContainerId = true;
    public boolean smartTree = true;
    public ExchangeRegistry exchangeRegistry = null;
    public QueueMapper queueMapper = null;

    public SwiftletContext(Configuration config, AMQPSwiftlet amqpSwiftlet) {
        this.config = config;
        this.amqpSwiftlet = amqpSwiftlet;
        root = config;
        usageList = (EntityList) root.getEntity("usage");
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
        protSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_PROTOCOL);
        logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
        accountingSwiftlet = (AccountingSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$accounting");
        authSwiftlet = (AuthenticationSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$authentication");
        timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");
        mgmtSwiftlet = (MgmtSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$mgmt");
        networkSwiftlet = (NetworkSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$net");
        queueManager = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
        topicManager = (TopicManager) SwiftletManager.getInstance().getSwiftlet("sys$topicmanager");
        threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
        transformerFactory = new TransformerFactory(this);
        Property prop = root.getProperty("allow-same-containerid");
        allowSameContainerId = ((Boolean) prop.getValue()).booleanValue();
        prop.setPropertyChangeListener(new PropertyChangeListener() {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                allowSameContainerId = ((Boolean) newValue).booleanValue();
            }
        });
        smartTree = SwiftletManager.getInstance().isUseSmartTree();
        if (smartTree)
            usageList.getTemplate().removeEntities();
        exchangeRegistry = new ExchangeRegistry(this);
        queueMapper = new QueueMapper(this);
    }

    public synchronized void addConnectId(String id) throws Exception {
        if (allowSameContainerId)
            return;
        if (connectedIds.contains(id))
            throw new Exception("Container-Id '" + id + "' is already connected");
        connectedIds.add(id);
    }

    public synchronized void removeId(String id) {
        if (allowSameContainerId)
            return;
        connectedIds.remove(id);
    }

}
