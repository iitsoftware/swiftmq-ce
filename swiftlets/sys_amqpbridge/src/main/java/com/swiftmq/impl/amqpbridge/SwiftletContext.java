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

package com.swiftmq.impl.amqpbridge;

import com.swiftmq.amqp.AMQPContext;
import com.swiftmq.amqp.integration.Tracer;
import com.swiftmq.impl.amqpbridge.v100.Poller;
import com.swiftmq.impl.amqpbridge.v100.RouterTracer;
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
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.topic.TopicManager;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;

public class SwiftletContext {
    static final String CONNECTION_TOKEN = "sys$amqpbridge.client.connection";
    static final String SESSION_TOKEN = "sys$amqpbridge.client.session";
    static final String POLL_TOKEN = "sys$amqpbridge.poller";

    public Configuration config = null;
    public Entity root = null;
    public Entity usage = null;
    public EntityList connectionTemplates = null;
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
    public AMQPBridgeSwiftlet bridgeSwiftlet = null;
    public AMQPContext amqpContext = null;
    public Poller poller = null;

    public SwiftletContext(Configuration config, AMQPBridgeSwiftlet bridgeSwiftlet) {
        this.config = config;
        this.bridgeSwiftlet = bridgeSwiftlet;
        root = config;
        usage = root.getEntity("usage");
        connectionTemplates = (EntityList) root.getEntity("connection-templates");
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_SWIFTLET);
        authSwiftlet = (AuthenticationSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$authentication");
        accountingSwiftlet = (AccountingSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$accounting");
        logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
        timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");
        mgmtSwiftlet = (MgmtSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$mgmt");
        queueManager = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
        topicManager = (TopicManager) SwiftletManager.getInstance().getSwiftlet("sys$topicmanager");
        threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
        schedulerSwiftlet = (SchedulerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$scheduler");
        amqpContext = new AMQPContext(AMQPContext.ROUTER) {
            public Tracer getFrameTracer() {
                return new RouterTracer(traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_PROTOCOL));
            }

            public Tracer getProcessingTracer() {
                return new RouterTracer(traceSpace);
            }

            public ThreadPool getConnectionPool() {
                return threadpoolSwiftlet.getPool(CONNECTION_TOKEN);
            }

            public ThreadPool getSessionPool() {
                return threadpoolSwiftlet.getPool(SESSION_TOKEN);
            }
        };
        poller = new Poller(threadpoolSwiftlet.getPool(POLL_TOKEN));
        poller.startQueue();
    }

    public void close() {
        poller.stopQueue();
    }
}
