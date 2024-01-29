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

package com.swiftmq.impl.queue.standard;

import com.swiftmq.impl.queue.standard.cluster.*;
import com.swiftmq.impl.queue.standard.composite.CompositeQueueFactory;
import com.swiftmq.impl.queue.standard.queue.CacheTableFactory;
import com.swiftmq.impl.queue.standard.queue.MessageQueue;
import com.swiftmq.impl.queue.standard.queue.MessageQueueFactory;
import com.swiftmq.mgmt.Configuration;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.AuthenticationSwiftlet;
import com.swiftmq.swiftlet.jndi.JNDISwiftlet;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.mgmt.MgmtSwiftlet;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.routing.RoutingSwiftlet;
import com.swiftmq.swiftlet.scheduler.SchedulerSwiftlet;
import com.swiftmq.swiftlet.store.StoreSwiftlet;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.topic.TopicManager;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;

public class SwiftletContext {
    public static final String PROP_LOG_EXPIRED = "log-expired-messages";
    public static final String PROP_LOG_DUPLICATES = "log-duplicate-messages";
    public static final String PROP_DELIVER_EXPIRED = "deliver-expired-messages";
    public static final String PROP_CLEANUP_INTERVAL = "cleanup-interval";
    public static final String PROP_COLLECT_INTERVAL = "collect-interval";
    public static final String PROP_MESSAGES_MAXIMUM = "max-messages";
    public static final String PROP_PERSISTENCE = "persistence-mode";
    public static final String PROP_MAX_FLOWCONTROL_DELAY = "max-flowcontrol-delay";
    public static final String PROP_FLOWCONTROL_QUEUE_SIZE = "flowcontrol-start-queuesize";
    public static final String PROP_ACACHE_SIZE = "acache-size";
    public static final String PROP_ACACHE_SIZE_KB = "acache-size-kb";
    public static final String PROP_ACLEANUP_INTERVAL = "acleanup-interval";
    public static final String PROP_AFLOWCONTROL_QUEUE_SIZE = "aflowcontrol-start-queuesize";
    public static final String PROP_AMESSAGES_MAXIMUM = "amax-messages";
    public static final String PROP_LATENCY = "latency";
    public static final String PROP_MCACHE_MESSAGES = "mcache-messages";
    public static final String PROP_MCACHE_SIZE_KB = "mcache-size-kb";
    public static final String PROP_MESSAGECOUNT = "messagecount";
    public static final String PROP_MSG_CONSUME_RATE = "msg-consume-rate";
    public static final String PROP_MSG_PRODUCE_RATE = "msg-produce-rate";
    public static final String PROP_TOTAL_CONSUMED = "total-consumed";
    public static final String PROP_TOTAL_PRODUCED = "total-produced";
    public static final String PROP_FLOWCONTROL_DELAY = "flowcontrol-delay";
    public static final String PROP_DUPLICATE_DETECTION_ENABLED = "duplicate-detection-enabled";
    public static final String PROP_DUPLICATE_DETECTION_BACKLOG_SIZE = "duplicate-detection-backlog-size";
    public static final String PROP_MULTI_QUEUE_TX_GLOBAL_LOCK = "multi-queue-transaction-global-lock";
    public static final String PROP_CONSUMER = "consumer-mode";
    static final String PROP_CACHE_SIZE = "cache-size";
    static final String PROP_CACHE_SIZE_BYTES_KB = "cache-size-bytes-kb";
    static SwiftletContext _ctx = null;
    public Configuration config = null;
    public Entity root = null;
    public EntityList usageList = null;
    public AuthenticationSwiftlet authSwiftlet = null;
    public JNDISwiftlet jndiSwiftlet = null;
    public EntityList jndiAliasList = null;
    public TimerSwiftlet timerSwiftlet = null;
    public MgmtSwiftlet mgmtSwiftlet = null;
    public LogSwiftlet logSwiftlet = null;
    public TraceSwiftlet traceSwiftlet = null;
    public TraceSpace traceSpace = null;
    public TraceSpace queueSpace = null;
    public QueueManagerImpl queueManager = null;
    public TopicManager topicManager = null;
    public ThreadpoolSwiftlet threadpoolSwiftlet = null;
    public StoreSwiftlet storeSwiftlet = null;
    public SchedulerSwiftlet schedulerSwiftlet = null;
    public RoutingSwiftlet routingSwiftlet = null;
    public boolean smartTree = false;
    public MessageQueueFactory messageQueueFactory = null;
    public CacheTableFactory cacheTableFactory = null;
    public DispatchPolicyRegistry dispatchPolicyRegistry = null;
    public RedispatcherController redispatcherController = null;
    public ClusterMetricPublisher clusterMetricPublisher = null;
    public ClusterMetricSubscriber clusterMetricSubscriber = null;
    public ClusteredQueueFactory clusteredQueueFactory = null;
    public CompositeQueueFactory compositeQueueFactory = null;
    public MessageGroupDispatchPolicyFactory messageGroupDispatchPolicyFactory = null;

    public SwiftletContext(QueueManagerImpl queueManager, Configuration config) {
        this.queueManager = queueManager;
        this.config = config;
        root = config;

        authSwiftlet = (AuthenticationSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$authentication");
        logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
        timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");
        threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
        storeSwiftlet = (StoreSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$store");
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
        queueSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_QUEUE);
        usageList = (EntityList) root.getEntity("usage");
        smartTree = SwiftletManager.getInstance().isUseSmartTree();
        clusteredQueueFactory = new ClusteredQueueFactory(this);
        compositeQueueFactory = new CompositeQueueFactory(this);
        _ctx = this;
    }

    public static SwiftletContext getContext() {
        return _ctx;
    }

    public int consumerModeInt(String mode) {
        if (mode.equals("exclusive"))
            return AbstractQueue.EXCLUSIVE;
        if (mode.equals("activestandby"))
            return AbstractQueue.ACTIVESTANDBY;
        return MessageQueue.SHARED;
    }
}
