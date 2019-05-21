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

import com.swiftmq.mgmt.Configuration;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.accounting.AccountingSwiftlet;
import com.swiftmq.swiftlet.auth.AuthenticationSwiftlet;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.mgmt.MgmtSwiftlet;
import com.swiftmq.swiftlet.queue.QueueManager;
import com.swiftmq.swiftlet.scheduler.SchedulerSwiftlet;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.topic.TopicManager;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;

public class SwiftletContext {
    public Configuration config = null;
    public Entity root = null;
    public EntityList usage = null;
    public AuthenticationSwiftlet authSwiftlet = null;
    public AccountingSwiftlet accountingSwiftlet = null;
    public ThreadpoolSwiftlet threadpoolSwiftlet = null;
    public TimerSwiftlet timerSwiftlet = null;
    public QueueManager queueManager = null;
    public TopicManager topicManager = null;
    public LogSwiftlet logSwiftlet = null;
    public TraceSwiftlet traceSwiftlet = null;
    public TraceSpace traceSpace = null;
    public MgmtSwiftlet mgmtSwiftlet = null;
    public SchedulerSwiftlet schedulerSwiftlet = null;
    public JMSBridgeSwiftlet bridgeSwiftlet = null;

    public SwiftletContext(Configuration config, JMSBridgeSwiftlet bridgeSwiftlet) {
        this.config = config;
        this.bridgeSwiftlet = bridgeSwiftlet;
        root = config;
        usage = (EntityList) root.getEntity("usage");
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
        authSwiftlet = (AuthenticationSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$authentication");
        accountingSwiftlet = (AccountingSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$accounting");
        logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
        timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");
        mgmtSwiftlet = (MgmtSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$mgmt");
        queueManager = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
        topicManager = (TopicManager) SwiftletManager.getInstance().getSwiftlet("sys$topicmanager");
        threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
        schedulerSwiftlet = (SchedulerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$scheduler");
    }
}
