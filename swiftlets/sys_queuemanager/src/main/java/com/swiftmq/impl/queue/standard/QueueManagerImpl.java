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
import com.swiftmq.impl.queue.standard.cluster.v700.QueueMetricImpl;
import com.swiftmq.impl.queue.standard.jobs.JobRegistrar;
import com.swiftmq.jms.DestinationFactory;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.auth.AuthenticationException;
import com.swiftmq.swiftlet.event.SwiftletManagerAdapter;
import com.swiftmq.swiftlet.event.SwiftletManagerEvent;
import com.swiftmq.swiftlet.jndi.JNDISwiftlet;
import com.swiftmq.swiftlet.mgmt.MgmtSwiftlet;
import com.swiftmq.swiftlet.mgmt.event.MgmtListener;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.swiftlet.queue.event.QueueManagerEvent;
import com.swiftmq.swiftlet.queue.event.QueueManagerListener;
import com.swiftmq.swiftlet.routing.RoutingSwiftlet;
import com.swiftmq.swiftlet.routing.event.RoutingEvent;
import com.swiftmq.swiftlet.routing.event.RoutingListenerAdapter;
import com.swiftmq.swiftlet.scheduler.SchedulerSwiftlet;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.swiftlet.topic.TopicManager;
import com.swiftmq.tools.sql.LikeComparator;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.versioning.Versionable;
import com.swiftmq.tools.versioning.Versioned;
import com.swiftmq.util.SwiftUtilities;

import java.io.IOException;
import java.util.*;

public class QueueManagerImpl extends QueueManager
        implements TimerListener {
    public static final String DLQ = "routerdlq";
    public static final String CLUSTER_TOPIC = "swiftmq.cluster";

    public static final String TP_TIMEOUTPROC = "sys$queuemanager.timeoutprocessor";
    public static final String TP_CLUSTER = "sys$queuemanager.cluster.subscriber";
    public static final String TP_REDISPATCHER = "sys$queuemanager.cluster.redispatcher";
    public static final int[] VERSIONS = {700};

    public static final String PROP_LOG_EXPIRED = "log-expired-messages";
    public static final String PROP_LOG_DUPLICATES = "log-duplicate-messages";
    public static final String PROP_DELIVER_EXPIRED = "deliver-expired-messages";
    public static final String PROP_CLEANUP_INTERVAL = "cleanup-interval";
    public static final String PROP_COLLECT_INTERVAL = "collect-interval";
    public static final String PROP_MESSAGES_MAXIMUM = "max-messages";
    public static final String PROP_PERSISTENCE = "persistence-mode";
    public static final String PROP_MAX_FLOWCONTROL_DELAY = "max-flowcontrol-delay";
    public static final String PROP_FLOWCONTROL_QUEUE_SIZE = "flowcontrol-start-queuesize";
    public static final String PROP_MONITOR_ALERT_THRESHOLD = "monitor-alert-threshold";
    public static final String PROP_ACACHE_SIZE = "acache-size";
    public static final String PROP_ACACHE_SIZE_KB = "acache-size-kb";
    public static final String PROP_ACLEANUP_INTERVAL = "acleanup-interval";
    public static final String PROP_AFLOWCONTROL_QUEUE_SIZE = "aflowcontrol-start-queuesize";
    public static final String PROP_AMESSAGES_MAXIMUM = "amax-messages";
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
    static final String PREFIX_TEMP_QUEUE = "tmp$";
    static final char SYSTEM_QUEUE_CHAR = '$';

    SwiftletContext ctx = null;
    EntityList queueControllerList = null;
    Entity tempQueueController = null;
    EntityListEventAdapter clusteredQueueAdapter = null;
    EntityListEventAdapter compositeQueueAdapter = null;
    ClusterRoutingListener routingListener = null;

    HashMap queueTable = null;
    HashMap inboundRedirectors = null;
    HashMap outboundRedirectors = null;

    JobRegistrar jobRegistrar = null;

    HashMap listeners = new HashMap();
    HashSet allQueueListeners = new HashSet();
    QueueFactory regularQueueFactory = null;
    QueueFactory tempQueueFactory = null;
    QueueFactory systemQueueFactory = null;
    boolean collectOn = false;
    long collectInterval = -1;
    long tmpCount = 0;
    boolean logExpired = true;
    boolean logDuplicates = true;
    boolean deliverExpired = true;
    volatile long maxFlowControlDelay = 5000;
    volatile long startupTime = 0;

    String localRouterName = null;

    protected Object qSemaphore = new Object();
    protected Object lSemaphore = new Object();

    long browserId = 0;
    long senderId = 0;
    long receiverId = 0;



    protected SwiftletContext createSwiftletContext(Configuration config) {
        return new SwiftletContext(this, config);
    }

    public MessageGroupDispatchPolicyFactory createMessaageGroupDispatchPolicyFactory() {
        return new MessageGroupDispatchPolicyFactoryImpl();
    }

    protected TempQueueFactory createTempQueueFactory() {
        return new TempQueueFactory(ctx);
    }

    protected SystemQueueFactory createSystemQueueFactory() {
        return new SystemQueueFactory(ctx);
    }

    protected RegularQueueFactory createRegularQueueFactory() {
        return new RegularQueueFactory(ctx);
    }

    protected MessageQueueFactory createMessageQueueFactory() {
        return new MessageQueueFactoryImpl();
    }

    protected CacheTableFactory createCacheTableFactory() {
        return new CacheTableFactoryImpl();
    }

    private Entity getQueueController(String queueName) {
        Entity entity = null;
        Map map = queueControllerList.getEntities();
        if (map != null && map.size() > 0) {
            for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); ) {
                Entity e = (Entity) ((Map.Entry) iter.next()).getValue();
                String predicate = (String) e.getProperty("predicate").getValue();
                if (LikeComparator.compare(queueName, predicate, '\\')) {
                    entity = e;
                    break;
                }
            }
        }
        return entity;
    }

    private String createTemporaryQueueName() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createTemporaryQueueName, cnt= " + tmpCount);
        StringBuffer b = new StringBuffer(PREFIX_TEMP_QUEUE);
        b.append(tmpCount++);
        b.append('-');
        b.append(startupTime);
        return b.toString();
    }

    public long getMaxFlowControlDelay() {
        return maxFlowControlDelay;
    }

    public boolean isLogDuplicates() {
        return logDuplicates;
    }

    public boolean isLogExpired() {
        return logExpired;
    }

    public boolean isDeliverExpired() {
        return deliverExpired;
    }

    public boolean isTemporaryQueue(String queueName) {
        return queueName.startsWith(PREFIX_TEMP_QUEUE);
    }

    public boolean isSystemQueue(String queueName) {
        return queueName.indexOf(SYSTEM_QUEUE_CHAR) != -1;
    }

    public String stripLocalName(String name) {
        String stripName = null;
        if (name.endsWith(localRouterName))
            stripName = name.substring(0, name.indexOf('@'));
        else
            stripName = name;
        return stripName;
    }

    private Entity getQueueEntity(String queueName) {
        EntityList queueList = (EntityList) ctx.root.getEntity("queues");
        return queueList.getEntity(queueName);
    }

    /**
     * @param queueName
     * @throws QueueException
     * @throws UnknownQueueException
     */
    private ActiveQueue getQueue(String queueName)
            throws UnknownQueueException, QueueException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "getQueue '" + queueName + "'");
        if (!isQueueDefined(queueName)) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "getQueue: Queue '" + queueName + "' is unknown");
            throw new UnknownQueueException("queue '" + queueName + "' is unknown");
        }
        ActiveQueue queue = (ActiveQueue) queueTable.get(queueName);
        if (queue == null) {
            String localName = stripLocalName(queueName);
            if (isTemporaryQueue(localName)) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "getQueue: Queue '" + localName + "' creating queue from tempQueueFactory");
                queue = new ActiveQueueImpl(tempQueueFactory.createQueue(localName, tempQueueController), tempQueueController);
            } else if (isSystemQueue(localName)) {
                Entity queueController = getQueueController(localName);
                if (queueController == null)
                    throw new QueueException("No matching Queue Controller found for queue: " + localName);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "getQueue: Queue '" + localName + "' creating queue from systemQueueFactory, using Queue Controller: " + queueController.getName());
                queue = new ActiveQueueImpl(systemQueueFactory.createQueue(localName, queueController), queueController);
            } else {
                Entity queueEntity = getQueueEntity(localName);
                if (queueEntity != null) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "getQueue: Queue '" + localName + "' creating queue from regularQueueFactory");
                    queue = new ActiveQueueImpl(regularQueueFactory.createQueue(localName, queueEntity), queueEntity);
                } else {
                    Entity queueController = getQueueController(localName);
                    if (queueController == null)
                        throw new QueueException("No matching Queue Controller found for queue: " + localName);
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "getQueue: Queue '" + localName + "' creating queue from systemQueueFactory, using Queue Controller: " + queueController.getName());
                    queue = new ActiveQueueImpl(systemQueueFactory.createQueue(localName, queueController), queueController);
                }
            }
            queue.getAbstractQueue().setQueueName(queueName);
            queue.getAbstractQueue().setLocalName(localName);
            queueTable.put(queueName, queue);
        }
        return queue;
    }

    /**
     * @param queueName
     * @throws QueueException
     * @throws UnknownQueueException
     */
    private ActiveQueue getQueue(String queueName, Entity queueEntity)
            throws UnknownQueueException, QueueException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "getQueue '" + queueName + "'");
        if (!isQueueDefined(queueName)) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "getQueue: Queue '" + queueName + "' is unknown");
            throw new UnknownQueueException("queue '" + queueName + "' is unknown");
        }
        ActiveQueue queue = (ActiveQueue) queueTable.get(queueName);
        if (queue == null) {
            String localName = stripLocalName(queueName);
            if (isTemporaryQueue(localName)) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "getQueue: Queue '" + localName + "' creating queue from tempQueueFactory");
                queue = new ActiveQueueImpl(tempQueueFactory.createQueue(localName, tempQueueController), tempQueueController);
            } else if (isSystemQueue(localName)) {
                Entity queueController = getQueueController(localName);
                if (queueController == null)
                    throw new QueueException("No matching Queue Controller found for queue: " + localName);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "getQueue: Queue '" + localName + "' creating queue from systemQueueFactory, using Queue Controller: " + queueController.getName());
                queue = new ActiveQueueImpl(systemQueueFactory.createQueue(localName, queueController), queueController);
            } else {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "getQueue: Queue '" + localName + "' creating queue from regularQueueFactory");
                queue = new ActiveQueueImpl(regularQueueFactory.createQueue(localName, queueEntity), queueEntity);
            }
            queue.getAbstractQueue().setQueueName(queueName);
            queue.getAbstractQueue().setLocalName(localName);
            queueTable.put(queueName, queue);
        }
        return queue;
    }

    public AbstractQueue getQueueForInternalUse(String queueName) {
        return getQueueForInternalUse(queueName, false);
    }

    public AbstractQueue getQueueForInternalUse(String queueName, boolean respectRedirection) {
        synchronized (qSemaphore) {
            queueName = fqn(queueName);
            if (respectRedirection)
                queueName = getRedirectedQueueName(outboundRedirectors, queueName);
            ActiveQueue queue = (ActiveQueue) queueTable.get(queueName);
            return queue == null ? null : queue.getAbstractQueue();
        }
    }

    public String fqn(String queueName) {
        if (queueName.indexOf('@') == -1)
            queueName += '@' + localRouterName;
        return queueName;
    }

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

    /**
     * @param queue
     * @throws QueueException
     */
    private void startQueue(ActiveQueue queue)
            throws QueueException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "startQueue: starting queue '" + queue.getAbstractQueue().getQueueName() + "'");
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "startQueue: queue '" + queue.getAbstractQueue().getQueueName() + "' fireQueueManagerEvent, method: queueStartInitiated");
        fireQueueManagerEvent(queue.getAbstractQueue().getQueueName(), "queueStartInitiated", new QueueManagerEvent(this, queue.getAbstractQueue().getQueueName()));
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "startQueue: queue '" + queue.getAbstractQueue().getQueueName() + "' queue.getAbstractQueue().startQueue()");
        queue.getAbstractQueue().startQueue();
        queue.setStartupTime(System.currentTimeMillis());
        Entity queueEntity = ((ActiveQueueImpl) queue).getQueueEntity();
        if (queueEntity != null) {
            Property prop = queueEntity.getProperty(PROP_CLEANUP_INTERVAL);
            CleanupListener l = new CleanupListener((ActiveQueueImpl) queue);
            prop.addPropertyWatchListener(l);
            ((ActiveQueueImpl) queue).setWatchListener(l);
        }
        long interval = queue.getAbstractQueue().getCleanUpInterval();
        if (interval > 0) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "startQueue: queue '" + queue.getAbstractQueue().getQueueName() + "' addTimerListener for interval " + interval);
            ctx.timerSwiftlet.addTimerListener(interval, queue);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "startQueue: queue '" + queue.getAbstractQueue().getQueueName() + "' fireQueueManagerEvent, method: queueStarted");
        fireQueueManagerEvent(queue.getAbstractQueue().getQueueName(), "queueStarted", new QueueManagerEvent(this, queue.getAbstractQueue().getQueueName()));
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "startQueue: queue '" + queue.getAbstractQueue().getQueueName() + "' started");
        if (ctx.jndiSwiftlet != null) {
            String name = queue.getAbstractQueue().getQueueName();
            try {
                SwiftUtilities.verifyQueueName(name);
                registerJNDI(name, new QueueImpl(name));
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * @param queue
     * @throws QueueException
     */
    private void stopQueue(ActiveQueue queue)
            throws QueueException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "stopQueue: stopping queue '" + queue.getAbstractQueue().getQueueName() + "'");
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "stopQueue: queue '" + queue.getAbstractQueue().getQueueName() + "' fireQueueManagerEvent, method: queueStopInitiated");
        fireQueueManagerEvent(queue.getAbstractQueue().getQueueName(), "queueStopInitiated", new QueueManagerEvent(this, queue.getAbstractQueue().getQueueName()));
        long interval = queue.getAbstractQueue().getCleanUpInterval();
        if (interval > 0) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "stopQueue: queue '" + queue.getAbstractQueue().getQueueName() + "' removeTimerListener for interval " + interval);
            ctx.timerSwiftlet.removeTimerListener(queue);
        }
        Entity queueEntity = ((ActiveQueueImpl) queue).getQueueEntity();
        if (queueEntity != null) {
            Property prop = queueEntity.getProperty(PROP_CLEANUP_INTERVAL);
            prop.removePropertyWatchListener(((ActiveQueueImpl) queue).getWatchListener());
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "stopQueue: queue '" + queue.getAbstractQueue().getQueueName() + "' queue.getAbstractQueue().stopQueue()");
        queue.getAbstractQueue().stopQueue();
        queue.setStartupTime(-1);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "stopQueue: queue '" + queue.getAbstractQueue().getQueueName() + "' fireQueueManagerEvent, method: queueStopped");
        fireQueueManagerEvent(queue.getAbstractQueue().getQueueName(), "queueStopped", new QueueManagerEvent(this, queue.getAbstractQueue().getQueueName()));
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "stopQueue: queue '" + queue.getAbstractQueue().getQueueName() + "' stopped");
        if (ctx.jndiSwiftlet != null) {
            String name = queue.getAbstractQueue().getQueueName();
            try {
                SwiftUtilities.verifyQueueName(name);
                ctx.jndiSwiftlet.deregisterJNDIObject(name);
                unregisterJNDIAlias(stripLocalName(name));
            } catch (Exception ignored) {
            }
        }
    }

    private void registerJNDIAlias(String alias, String mapTo) {
        try {
            if (ctx.jndiAliasList.getEntity(alias) != null)
                return;
            Entity entity = ctx.jndiAliasList.createEntity();
            entity.setName(alias);
            entity.getProperty("map-to").setValue(mapTo);
            entity.createCommands();
            ctx.jndiAliasList.addEntity(entity);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void unregisterJNDIAlias(String alias) {
        try {
            Entity entity = ctx.jndiAliasList.getEntity(alias);
            if (entity != null)
                ctx.jndiAliasList.removeEntity(entity);
        } catch (EntityRemoveException e) {
            e.printStackTrace();
        }
    }

    private void registerJNDIQueues() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "registering JNDI queues ...");
        synchronized (qSemaphore) {
            Iterator iter = queueTable.entrySet().iterator();
            while (iter.hasNext()) {
                ActiveQueue activeQueue = (ActiveQueue) ((Map.Entry) iter.next()).getValue();
                if (activeQueue.getStartupTime() != -1) {
                    try {
                        String fqName = activeQueue.getAbstractQueue().getQueueName();
                        SwiftUtilities.verifyQueueName(fqName);
                        registerJNDI(fqName, new QueueImpl(fqName));
                    } catch (Exception ignored) {
                    }
                }
            }
        }
    }

    private void registerJNDI(String name, QueueImpl queue) {
        try {
            String localName = stripLocalName(name);
            DataByteArrayOutputStream dos = new DataByteArrayOutputStream();
            DestinationFactory.dumpDestination(queue, dos);
            Versionable versionable = new Versionable();
            versionable.addVersioned(-1, new Versioned(-1, dos.getBuffer(), dos.getCount()), "com.swiftmq.jms.DestinationFactory");
            ctx.jndiSwiftlet.registerJNDIObject(name, versionable);
            registerJNDIAlias(localName, name);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param queueName
     */
    public boolean isQueueDefined(String queueName) {
        queueName = fqn(queueName);
        boolean b = false;
        synchronized (qSemaphore) {
            b = queueTable.containsKey(queueName);
        }
        return b;
    }

    /**
     * @param queueName
     */
    public boolean isQueueRunning(String queueName) {
        queueName = fqn(queueName);
        boolean b = false;
        synchronized (qSemaphore) {
            queueName = getRedirectedQueueName(outboundRedirectors, queueName);
            ActiveQueue queue = (ActiveQueue) queueTable.get(queueName);
            b = queue != null && queue.getAbstractQueue().isRunning();
        }
        return b;
    }

    /**
     * @param queueName
     * @param activeLogin
     * @throws QueueException
     * @throws AuthenticationException
     * @throws UnknownQueueException
     */
    public QueueSender createQueueSender(String queueName, ActiveLogin activeLogin)
            throws QueueException, AuthenticationException, UnknownQueueException {
        queueName = fqn(queueName);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "createQueueSender '" + queueName + "', activeLogin: " + activeLogin);
        ActiveQueue queue = null;
        long id = 0;
        synchronized (qSemaphore) {
            if (activeLogin != null && !isSystemQueue(queueName) && !isTemporaryQueue(queueName)) {
                try {
                    ctx.authSwiftlet.verifyQueueSenderSubscription(queueName, activeLogin.getLoginId());
                } catch (AuthenticationException e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "createQueueSender '" + queueName + "', User: '" +
                                activeLogin.getUserName() + "', AuthenticationException: " + e.getMessage());
                    ctx.logSwiftlet.logError(getName(), "createQueueSender '" + queueName + "', User: '" +
                            activeLogin.getUserName() + "', AuthenticationException: " + e.getMessage());
                    throw e;
                }
            }
            queueName = getRedirectedQueueName(outboundRedirectors, queueName);
            queue = getQueue(queueName);
            if (!queue.getAbstractQueue().isRunning())
                startQueue(queue);
            id = senderId++;
            if (senderId == Long.MAX_VALUE)
                senderId = 0;
        }
        QueueSender sender = null;
        try {
            if (ctx.smartTree || queueName.startsWith("tpc$"))
                sender = new QueueSender(queue, null);
            else {
                Entity queueEntity = ctx.usageList.getEntity(stripLocalName(queueName));
                EntityList list = (EntityList) queueEntity.getEntity("sender");
                Entity entity = list.createEntity();
                entity.setName(String.valueOf(id));
                entity.createCommands();
                Property prop = entity.getProperty("username");
                prop.setValue(activeLogin != null ? activeLogin.getUserName() : "Internal Swiftlet Usage");
                prop.setReadOnly(true);
                prop = entity.getProperty("clientid");
                prop.setValue(activeLogin != null ? activeLogin.getClientId() : "Internal Swiftlet Usage");
                prop.setReadOnly(true);
                sender = new QueueSender(queue, list);
                entity.setDynamicObject(sender);
                list.addEntity(entity);
            }
            addQueueManagerListener(queueName, sender);
        } catch (Exception e) {
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "createQueueSender '" + queueName + "', activeLogin: " + activeLogin + " successful");
        return sender;
    }

    /**
     * @param queueName
     * @param activeLogin
     * @throws QueueException
     * @throws AuthenticationException
     * @throws UnknownQueueException
     */
    public QueueReceiver createQueueReceiver(String queueName, ActiveLogin activeLogin, Selector selector)
            throws QueueException, AuthenticationException, UnknownQueueException {
        queueName = fqn(queueName);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "createQueueReceiver '" + queueName + "', activeLogin: " + activeLogin);
        ActiveQueue queue = null;
        long id = 0;
        synchronized (qSemaphore) {
            if (activeLogin != null && !isSystemQueue(queueName) && !isTemporaryQueue(queueName)) {
                try {
                    ctx.authSwiftlet.verifyQueueReceiverSubscription(queueName, activeLogin.getLoginId());
                } catch (AuthenticationException e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "createQueueReceiver '" + queueName + "', User: '" +
                                activeLogin.getUserName() + "', AuthenticationException: " + e.getMessage());
                    ctx.logSwiftlet.logError(getName(), "createQueueReceiver '" + queueName + "', User: '" +
                            activeLogin.getUserName() + "', AuthenticationException: " + e.getMessage());
                    throw e;
                }
            }
            queueName = getRedirectedQueueName(inboundRedirectors, queueName);
            queue = getQueue(queueName);
            if (!queue.getAbstractQueue().isRunning())
                startQueue(queue);
            if (queue.getAbstractQueue().getConsumerMode() == AbstractQueue.EXCLUSIVE &&
                    queue.getAbstractQueue().getReceiverCount() > 0)
                throw new QueueException("Can't create new consumer - queue is EXCLUSIVE and has already a consumer");
            id = receiverId++;
            if (receiverId == Long.MAX_VALUE)
                receiverId = 0;
        }
        QueueReceiver receiver = null;
        try {
            if (ctx.smartTree)
                receiver = new QueueReceiver(queue, null, selector);
            else {
                Entity queueEntity = ctx.usageList.getEntity(stripLocalName(queueName));
                EntityList list = (EntityList) queueEntity.getEntity("receiver");
                Entity entity = list.createEntity();
                entity.setName(String.valueOf(id));
                entity.createCommands();
                Property prop = entity.getProperty("username");
                prop.setValue(activeLogin != null ? activeLogin.getUserName() : "Internal Swiftlet Usage");
                prop.setReadOnly(true);
                prop = entity.getProperty("clientid");
                prop.setValue(activeLogin != null ? activeLogin.getClientId() : "Internal Swiftlet Usage");
                prop.setReadOnly(true);
                prop = entity.getProperty("selector");
                if (selector != null) {
                    prop.setValue(selector.getConditionString());
                }
                prop.setReadOnly(true);
                receiver = new QueueReceiver(queue, list, selector);
                entity.setDynamicObject(receiver);
                list.addEntity(entity);
            }
            receiver.setReceiverId(id);
            addQueueManagerListener(receiver.getQueueName(), receiver);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "createQueueReceiver '" + queueName + "', activeLogin: " + activeLogin + " successful");
        return receiver;
    }

    /**
     * @param queueName
     * @param activeLogin
     * @param selector
     * @throws QueueException
     * @throws AuthenticationException
     * @throws UnknownQueueException
     */
    public QueueBrowser createQueueBrowser(String queueName, ActiveLogin activeLogin, Selector selector)
            throws QueueException, AuthenticationException, UnknownQueueException {
        queueName = fqn(queueName);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "createQueueBrowser '" + queueName + "', activeLogin: " + activeLogin + ", selector: " + selector);
        queueName = getRedirectedQueueName(inboundRedirectors, queueName);
        ActiveQueue queue = null;
        long id = 0;
        synchronized (qSemaphore) {
            queue = getQueue(queueName);
            if (activeLogin != null) {
                try {
                    ctx.authSwiftlet.verifyQueueBrowserCreation(queueName, activeLogin.getLoginId());
                } catch (AuthenticationException e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "createQueueBrowser '" + queueName + "', User: '" +
                                activeLogin.getUserName() + "', AuthenticationException: " + e.getMessage());
                    ctx.logSwiftlet.logError(getName(), "createQueueBrowser '" + queueName + "', User: '" +
                            activeLogin.getUserName() + "', AuthenticationException: " + e.getMessage());
                    throw e;
                }
            }
            if (!queue.getAbstractQueue().isRunning())
                startQueue(queue);
            id = browserId++;
            if (browserId == Long.MAX_VALUE)
                browserId = 0;
        }
        QueueBrowser browser = null;
        try {
            if (ctx.smartTree)
                browser = new QueueBrowser(queue, selector, null);
            else {
                Entity queueEntity = ctx.usageList.getEntity(stripLocalName(queueName));
                EntityList list = (EntityList) queueEntity.getEntity("browser");
                Entity entity = list.createEntity();
                entity.setName(String.valueOf(id));
                entity.createCommands();
                Property prop = entity.getProperty("username");
                prop.setValue(activeLogin != null ? activeLogin.getUserName() : "Internal Swiftlet Usage");
                prop.setReadOnly(true);
                prop = entity.getProperty("clientid");
                prop.setValue(activeLogin != null ? activeLogin.getClientId() : "Internal Swiftlet Usage");
                prop.setReadOnly(true);
                prop = entity.getProperty("selector");
                if (selector != null) {
                    prop.setValue(selector.getConditionString());
                }
                prop.setReadOnly(true);
                browser = new QueueBrowser(queue, selector, list);
                entity.setDynamicObject(browser);
                list.addEntity(entity);
            }
            addQueueManagerListener(queueName, browser);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "createQueueBrowser '" + queueName + "', activeLogin: " + activeLogin + " successful");
        return browser;
    }

    public void addQueueManagerListener(QueueManagerListener l) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "addQueueManagerListener for all queues");
        synchronized (lSemaphore) {
            allQueueListeners.add(l);
        }
    }

    public void addQueueManagerListener(String queueName, QueueManagerListener l)
            throws UnknownQueueException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "addQueueManagerListener '" + queueName + "'");
        if (!isQueueDefined(queueName)) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "addQueueManagerListener: Queue '" + queueName + "' is unknown");
            throw new UnknownQueueException("queue '" + queueName + "' is unknown");
        }
        synchronized (lSemaphore) {
            HashSet qListeners = (HashSet) listeners.get(queueName);
            if (qListeners == null) {
                qListeners = new HashSet();
                listeners.put(queueName, qListeners);
            }
            qListeners.add(l);
        }
    }

    public void removeQueueManagerListener(QueueManagerListener l) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "removeQueueManagerListener for all queues");
        synchronized (lSemaphore) {
            allQueueListeners.remove(l);
        }
    }

    public void removeQueueManagerListener(String queueName, QueueManagerListener l)
            throws UnknownQueueException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "removeQueueManagerListener '" + queueName + "'");
        if (!isQueueDefined(queueName)) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "removeQueueManagerListener: Queue '" + queueName + "' is unknown");
            throw new UnknownQueueException("queue '" + queueName + "' is unknown");
        }
        synchronized (lSemaphore) {
            HashSet qListeners = (HashSet) listeners.get(queueName);
            if (qListeners != null) {
                qListeners.remove(l);
                if (qListeners.isEmpty())
                    listeners.put(queueName, null);
            }
        }
    }

    private void fireQueueManagerEvent(String queueName, String methodName, QueueManagerEvent evt) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "fireQueueManagerEvent '" + queueName + "', methodName: " + methodName);
        ArrayList myListeners = new ArrayList();
        synchronized (lSemaphore) {
            for (Iterator iter = allQueueListeners.iterator(); iter.hasNext(); ) {
                myListeners.add(iter.next());
            }
            HashSet qListeners = (HashSet) listeners.get(queueName);
            if (qListeners != null) {
                for (Iterator iter = qListeners.iterator(); iter.hasNext(); ) {
                    myListeners.add(iter.next());
                }
            }
        }
        for (int i = 0; i < myListeners.size(); i++) {
            QueueManagerListener l = (QueueManagerListener) myListeners.get(i);
            if (methodName.equals("queueStartInitiated"))
                l.queueStartInitiated(evt);
            else if (methodName.equals("queueStarted"))
                l.queueStarted(evt);
            else if (methodName.equals("queueStopInitiated"))
                l.queueStopInitiated(evt);
            else if (methodName.equals("queueStopped"))
                l.queueStopped(evt);
        }
    }

    private void createQueue(Entity queueEntity) throws Exception {
        ActiveQueue queue = null;
        synchronized (qSemaphore) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createQueue: queue '" + queueEntity.getName());
            SwiftUtilities.verifyLocalQueueName(queueEntity.getName());
            String qn = queueEntity.getName() + '@' + localRouterName;
            queueTable.put(qn, null);
            queue = getQueue(qn, queueEntity);
            startQueue(queue);
        }
        Entity qEntity = ctx.usageList.createEntity();
        qEntity.setName(queueEntity.getName());
        qEntity.setDynamicObject(queue);
        qEntity.createCommands();
        ctx.usageList.addEntity(qEntity);
    }

    private void createQueues(EntityList queueList) throws SwiftletException {
        Map m = queueList.getEntities();
        if (m.size() > 0) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createQueues: begin starting queues ...");
            try {
                for (Iterator iter = m.entrySet().iterator(); iter.hasNext(); ) {
                    Entity queueEntity = (Entity) ((Map.Entry) iter.next()).getValue();
                    createQueue(queueEntity);
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new SwiftletException(e.getMessage());
            }
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createQueues: end starting queues ...");
        } else if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createQueues: no queues defined");

        queueList.setEntityAddListener(new EntityChangeAdapter(null) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                try {
                    createQueue(newEntity);
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "onEntityAdd (queue): new queue=" + newEntity.getName());
                } catch (Exception e) {
                    throw new EntityAddException(e.getMessage());
                }
            }
        });
        queueList.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                try {
                    String qn = delEntity.getName() + '@' + localRouterName;
                    ActiveQueue queue = getQueue(qn);
                    queue.getAbstractQueue().deleteContent();
                    deleteQueue(qn, false);
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "onEntityRemove (queue): del queue=" + delEntity.getName());
                } catch (Exception e) {
                    throw new EntityRemoveException(e.getMessage());
                }
            }
        });
    }

    public void performTimeAction() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "collecting message counts...");
        Map cloned = null;
        synchronized (qSemaphore) {
            cloned = (Map) queueTable.clone();
        }
        for (Iterator iter = cloned.entrySet().iterator(); iter.hasNext(); ) {
            ActiveQueue queue = (ActiveQueue) ((Map.Entry) iter.next()).getValue();
            if (queue != null) {
                AbstractQueue ac = queue.getAbstractQueue();
                if (ac != null && ac.isRunning()) {
                    String queueName = ac.getLocalName();
                    Entity queueEntity = ctx.usageList.getEntity(queueName);
                    if (queueEntity != null) {
                        try {
                            Property prop = queueEntity.getProperty(PROP_MESSAGECOUNT);
                            int oldValue = ((Integer) prop.getValue()).intValue();
                            int actValue = (int) ac.getNumberQueueMessages();
                            if (oldValue != actValue) {
                                prop.setValue(new Integer(actValue));
                            }
                            prop = queueEntity.getProperty(PROP_MSG_CONSUME_RATE);
                            int oldCR = ((Integer) prop.getValue()).intValue();
                            int actCR = ac.getConsumingRate();
                            if (oldCR != actCR) {
                                prop.setValue(new Integer(actCR));
                            }
                            prop = queueEntity.getProperty(PROP_MSG_PRODUCE_RATE);
                            int oldPR = ((Integer) prop.getValue()).intValue();
                            int actPR = ac.getProducingRate();
                            if (oldPR != actPR) {
                                prop.setValue(new Integer(actPR));
                            }
                            prop = queueEntity.getProperty(PROP_TOTAL_CONSUMED);
                            int oldTC = ((Integer) prop.getValue()).intValue();
                            int actTC = ac.getConsumedTotal();
                            if (oldTC != actTC) {
                                prop.setValue(new Integer(actTC));
                            }
                            prop = queueEntity.getProperty(PROP_TOTAL_PRODUCED);
                            int oldTP = ((Integer) prop.getValue()).intValue();
                            int actTP = ac.getProducedTotal();
                            if (oldTP != actTP) {
                                prop.setValue(new Integer(actTP));
                            }
                            prop = queueEntity.getProperty(PROP_MCACHE_MESSAGES);
                            int oldCM = ((Integer) prop.getValue()).intValue();
                            int actCM = ac.getCurrentCacheSizeMessages();
                            if (oldCM != actCM) {
                                prop.setValue(new Integer(actCM));
                            }
                            prop = queueEntity.getProperty(PROP_MCACHE_SIZE_KB);
                            int oldS = ((Integer) prop.getValue()).intValue();
                            int actS = ac.getCurrentCacheSizeKB();
                            if (oldS != actS) {
                                prop.setValue(new Integer(actS));
                            }
                            prop = queueEntity.getProperty(PROP_FLOWCONTROL_DELAY);
                            long oldDelay = ((Long) prop.getValue()).longValue();
                            long actDelay = 0;
                            FlowController fc = ac.getFlowController();
                            if (fc != null)
                                actDelay = fc.getLastDelay();
                            if (oldDelay != actValue) {
                                prop.setValue(new Long(actDelay));
                            }
                            prop = queueEntity.getProperty(PROP_AMESSAGES_MAXIMUM);
                            int oldMax = ((Integer) prop.getValue()).intValue();
                            int actMax = ac.getMaxMessages();
                            if (oldMax != actMax) {
                                prop.setValue(new Integer(actMax));
                            }
                            prop = queueEntity.getProperty(PROP_ACACHE_SIZE);
                            int oldSize = ((Integer) prop.getValue()).intValue();
                            int actSize = ac.getCacheSize();
                            if (oldSize != actSize) {
                                prop.setValue(new Integer(actSize));
                            }
                            prop = queueEntity.getProperty(PROP_ACACHE_SIZE_KB);
                            int oldSizeKB = ((Integer) prop.getValue()).intValue();
                            int actSizeKB = ac.getCacheSizeKB();
                            if (oldSizeKB != actSizeKB) {
                                prop.setValue(new Integer(actSizeKB));
                            }
                            prop = queueEntity.getProperty(PROP_AFLOWCONTROL_QUEUE_SIZE);
                            int oldFC = ((Integer) prop.getValue()).intValue();
                            int actFC = 0;
                            fc = ac.getFlowController();
                            if (fc != null)
                                actFC = fc.getStartQueueSize();
                            if (oldFC != actFC) {
                                prop.setValue(new Integer(actFC));
                            }
                            prop = queueEntity.getProperty(PROP_ACLEANUP_INTERVAL);
                            long oldCU = ((Long) prop.getValue()).longValue();
                            long actCU = ac.getCleanUpInterval();
                            if (oldCU != actCU) {
                                prop.setValue(new Long(actCU));
                            }
                        } catch (Exception ignored) {
                            ignored.printStackTrace();
                        }
                    }
                }
            }
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "collecting message counts...DONE.");
    }

    /**
     * @param queueName
     * @param activeLogin
     * @throws QueueException
     * @throws QueueAlreadyDefinedException
     */
    public void createQueue(String queueName, ActiveLogin activeLogin)
            throws QueueException, QueueAlreadyDefinedException, AuthenticationException {
        queueName = fqn(queueName);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createQueue '" + queueName + "'");
        if (isQueueDefined(queueName))
            throw new QueueAlreadyDefinedException("queue '" + queueName + "' is already defined!");
        ActiveQueue queue = null;
        synchronized (qSemaphore) {
            if (activeLogin != null) {
                try {
                    ctx.authSwiftlet.verifyQueueCreation(activeLogin.getLoginId());
                } catch (AuthenticationException e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "createQueue '" + queueName + "', User: '" +
                                activeLogin.getUserName() + "', AuthenticationException: " + e.getMessage());
                    ctx.logSwiftlet.logError(getName(), "createQueue '" + queueName + "', User: '" +
                            activeLogin.getUserName() + "', AuthenticationException: " + e.getMessage());
                    throw e;
                }
            }
            queueTable.put(queueName, null);
            try {
                queue = getQueue(queueName);
            } catch (UnknownQueueException e) {
            }
            startQueue(queue);
        }
        try {
            Entity qEntity = ctx.usageList.createEntity();
            qEntity.setName(stripLocalName(queueName));
            qEntity.setDynamicObject(queue);
            qEntity.createCommands();
            ctx.usageList.addEntity(qEntity);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createQueue(String queueName, Entity queueEntity, QueueFactory factory)
            throws QueueException {
        queueName = fqn(queueName);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createQueue '" + queueName + "'");
        String localName = null;
        ActiveQueue queue = null;
        synchronized (qSemaphore) {
            if (isQueueDefined(queueName))
                throw new QueueAlreadyDefinedException("queue '" + queueName + "' is already defined");
            localName = stripLocalName(queueName);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "createQueue: Queue '" + localName + "' creating queue from queueFactory");
            queue = new ActiveQueueImpl(factory.createQueue(localName, queueEntity), null);
            queue.getAbstractQueue().setQueueName(queueName);
            queue.getAbstractQueue().setLocalName(localName);
            queueTable.put(queueName, queue);
            startQueue(queue);
        }
        if (factory.registerUsage()) {
            try {
                Entity qEntity = ctx.usageList.createEntity();
                qEntity.setName(localName);
                qEntity.setDynamicObject(queue);
                qEntity.createCommands();
                ctx.usageList.addEntity(qEntity);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void createQueue(String queueName, QueueFactory factory)
            throws QueueException, QueueAlreadyDefinedException {
        queueName = fqn(queueName);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createQueue '" + queueName + "'");
        String localName = null;
        ActiveQueue queue = null;
        synchronized (qSemaphore) {
            if (isQueueDefined(queueName))
                throw new QueueAlreadyDefinedException("queue '" + queueName + "' is already defined");
            localName = stripLocalName(queueName);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "createQueue: Queue '" + localName + "' creating queue from queueFactory");
            queue = new ActiveQueueImpl(factory.createQueue(localName, getQueueEntity(localName)), null);
            queue.getAbstractQueue().setQueueName(queueName);
            queue.getAbstractQueue().setLocalName(localName);
            queueTable.put(queueName, queue);
            startQueue(queue);
        }
        if (factory.registerUsage()) {
            try {
                Entity qEntity = ctx.usageList.createEntity();
                qEntity.setName(localName);
                qEntity.setDynamicObject(queue);
                qEntity.createCommands();
                ctx.usageList.addEntity(qEntity);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * @param queueName
     * @param onEmpty
     * @throws UnknownQueueException
     * @throws QueueException
     */
    public void deleteQueue(String queueName, boolean onEmpty)
            throws UnknownQueueException, QueueException {
        queueName = fqn(queueName);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "deleteQueue '" + queueName + "'");
        boolean deleted = false;
        ActiveQueue queue = null;
        synchronized (qSemaphore) {
            queue = getQueue(queueName);
            if (!onEmpty || onEmpty && queue.getAbstractQueue().getNumberQueueMessages() == 0) {
                queue.getAbstractQueue().deleteContent();
                if (queue.getAbstractQueue().isRunning())
                    stopQueue(queue);
                queueTable.remove(queueName);
                deleted = true;
            }
        }
        if (deleted) {
            synchronized (lSemaphore) {
                listeners.remove(queueName);
            }
            ctx.usageList.removeDynamicEntity(queue);
        }
    }

    /**
     * @throws QueueException
     */
    public String createTemporaryQueue()
            throws QueueException {
        String queueName = null;
        String tmpQueueName = null;
        ActiveQueue queue = null;
        synchronized (qSemaphore) {
            queueName = createTemporaryQueueName();
            tmpQueueName = queueName + '@' + localRouterName;
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createTemporaryQueue '" + tmpQueueName + "'");
            queueTable.put(tmpQueueName, null);
            try {
                queue = getQueue(tmpQueueName);
            } catch (UnknownQueueException e) {
            }
            // mark queue as temporary
            queue.getAbstractQueue().setTemporary(true);
            startQueue(queue);
        }
        try {
            Entity qEntity = ctx.usageList.createEntity();
            qEntity.setName(stripLocalName(queueName));
            qEntity.setDynamicObject(queue);
            qEntity.createCommands();
            ctx.usageList.addEntity(qEntity);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tmpQueueName;
    }

    /**
     * @param queueName
     * @throws UnknownQueueException
     * @throws QueueException
     */
    public void deleteTemporaryQueue(String queueName)
            throws UnknownQueueException, QueueException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "deleteTemporaryQueue '" + queueName + "'");
        ActiveQueue queue = null;
        synchronized (qSemaphore) {
            queue = getQueue(queueName);
            if (queue.getAbstractQueue().isRunning()) {
                stopQueue(queue);
            }
            queueTable.remove(queueName);
        }
        synchronized (lSemaphore) {
            listeners.remove(queueName);
        }
        ctx.usageList.removeDynamicEntity(queue);
    }

    /**
     * @param queueName
     * @throws UnknownQueueException
     * @throws QueueException
     */
    public void purgeQueue(String queueName)
            throws UnknownQueueException, QueueException {
        queueName = fqn(queueName);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "purgeQueue '" + queueName + "'");
        synchronized (qSemaphore) {
            ActiveQueue queue = getQueue(queueName);
            if (queue.getAbstractQueue().isRunning()) {
                queue.getAbstractQueue().deleteContent();
            }
        }
    }

    public String[] getDefinedQueueNames() {
        String[] names = null;
        synchronized (qSemaphore) {
            if (queueTable.size() > 0) {
                names = new String[queueTable.size()];
                Set set = queueTable.keySet();
                Iterator iter = set.iterator();
                int i = 0;
                while (iter.hasNext())
                    names[i++] = (String) iter.next();
            }
        }
        return names;
    }

    private String getRedirectedQueueName(HashMap redirectorTable, String queueName) {
        String name = queueName;
        synchronized (qSemaphore) {
            Iterator iter = redirectorTable.keySet().iterator();
            while (iter.hasNext()) {
                String predicate = (String) iter.next();
                if (LikeComparator.compare(queueName, predicate, '\\')) {
                    name = (String) redirectorTable.get(predicate);
                    break;
                }
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "getRedirectedQueueName '" + queueName + "' returns '" + name + "'");
        return name;
    }

    /**
     * @param outboundQueueName
     * @param likePredicate
     * @throws UnknownQueueException
     */
    public void setQueueOutboundRedirector(String likePredicate, String outboundQueueName)
            throws UnknownQueueException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "setQueueOutboundRedirector, likePredicate '" + likePredicate + "' outboundQueueName '" + outboundQueueName + "'");
        synchronized (qSemaphore) {
            if (outboundQueueName == null)
                outboundRedirectors.remove(likePredicate);
            else {
                if (!isQueueDefined(outboundQueueName))
                    throw new UnknownQueueException("queue '" + outboundQueueName + "' is unknown!");
                outboundRedirectors.put(likePredicate, outboundQueueName);
            }
        }
    }

    /**
     * @param likePredicate
     * @param inboundQueueName
     * @throws UnknownQueueException
     */
    public void setQueueInboundRedirector(String likePredicate, String inboundQueueName)
            throws UnknownQueueException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "setQueueInboundRedirector, likePredicate '" + likePredicate + "' inboundQueueName '" + inboundQueueName + "'");
        synchronized (qSemaphore) {
            if (inboundQueueName == null)
                inboundRedirectors.remove(likePredicate);
            else {
                if (!isQueueDefined(inboundQueueName))
                    throw new UnknownQueueException("queue '" + inboundQueueName + "' is unknown!");
                inboundRedirectors.put(likePredicate, inboundQueueName);
            }
        }
    }

    private void createClusteredQueue(Entity entity) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "createClusteredQueue, name=" + entity.getName() + " ...");
        createQueue(entity.getName(), entity, ctx.clusteredQueueFactory);

        EntityListEventAdapter bindingsAdapter = new EntityListEventAdapter((EntityList) entity.getEntity("queue-bindings"), true, true) {
            public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd (bindings), name=" + newEntity.getName() + " ...");
                String qn = fqn(newEntity.getName());
                if (!isQueueDefined(qn))
                    throw new EntityAddException("Undefined queue: " + qn + "! To bind a queue to a clustered queue, it must be defined first.");
                AbstractQueue queue = getQueueForInternalUse(qn);
                String clusteredQueueName = parent.getParent().getName();
                DispatchPolicy dp = ctx.dispatchPolicyRegistry.get(clusteredQueueName);
                queue.setQueueReceiverListener(dp);
                Property prop = newEntity.getProperty("redispatch-enabled");
                prop.setPropertyChangeListener(new PropertyChangeListener() {
                    public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                        ((QueueMetric) property.getParent().getUserObject()).setRedispatch(((Boolean) newValue).booleanValue());
                    }
                });
                QueueMetric qm = new QueueMetricImpl(qn, queue.getReceiverCount() > 0, ((Boolean) prop.getValue()).booleanValue());
                dp.addLocalMetric(qm);
                newEntity.setUserObject(qm);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd (bindings), name=" + newEntity.getName() + " done");
            }

            public void onEntityRemove(Entity parent, Entity oldEntity) throws EntityRemoveException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove (bindings), name=" + oldEntity.getName() + " ...");
                String qn = fqn(oldEntity.getName());
                if (isQueueDefined(qn)) {
                    AbstractQueue queue = getQueueForInternalUse(qn);
                    queue.setQueueReceiverListener(null);
                    String clusteredQueueName = parent.getParent().getName();
                    DispatchPolicy dp = ctx.dispatchPolicyRegistry.get(clusteredQueueName);
                    dp.removeLocalMetric((QueueMetric) oldEntity.getUserObject());
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove (bindings), name=" + oldEntity.getName() + " done");
            }
        };
        bindingsAdapter.init();
        entity.setUserObject(bindingsAdapter);

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "createClusteredQueue, name=" + entity.getName() + " done");
    }

    private void deleteClusteredQueue(Entity entity) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "deleteClusteredQueue, name=" + entity.getName() + " ...");
        ((EntityListEventAdapter) entity.getUserObject()).close();
        ctx.dispatchPolicyRegistry.remove(entity.getName());
        deleteQueue(entity.getName(), false);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "deleteClusteredQueue, name=" + entity.getName() + " done");
    }

    protected void startCluster() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startCluster ...");
        ctx.topicManager = (TopicManager) SwiftletManager.getInstance().getSwiftlet("sys$topicmanager");
        if (!ctx.topicManager.isTopicDefined(CLUSTER_TOPIC))
            ctx.topicManager.createTopic(CLUSTER_TOPIC);
        ctx.clusterMetricPublisher = new ClusterMetricPublisher(ctx);
        ctx.clusterMetricSubscriber = new ClusterMetricSubscriber(ctx);

        clusteredQueueAdapter = new EntityListEventAdapter((EntityList) ctx.root.getEntity("clustered-queues"), true, true) {
            public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException {
                try {
                    createClusteredQueue(newEntity);
                } catch (Exception e) {
                    throw new EntityAddException(e.getMessage());
                }
            }

            public void onEntityRemove(Entity parent, Entity oldEntity) throws EntityRemoveException {
                try {
                    deleteClusteredQueue(oldEntity);
                } catch (Exception e) {
                    throw new EntityRemoveException(e.getMessage());
                }
            }
        };
        clusteredQueueAdapter.init();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startCluster done");
    }

    private void stopCluster() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "stopCluster ...");
        clusteredQueueAdapter.close();
        ctx.clusterMetricPublisher.close();
        ctx.clusterMetricSubscriber.close();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "stopCluster done");
    }

    private void startCompositeQueues() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startCompositeQueues ...");
        compositeQueueAdapter = new EntityListEventAdapter((EntityList) ctx.root.getEntity("composite-queues"), true, true) {
            public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException {
                try {
                    createQueue(newEntity.getName(), newEntity, ctx.compositeQueueFactory);
                } catch (Exception e) {
                    throw new EntityAddException(e.getMessage());
                }
            }

            public void onEntityRemove(Entity parent, Entity oldEntity) throws EntityRemoveException {
                try {
                    deleteQueue(oldEntity.getName(), false);
                } catch (Exception e) {
                    throw new EntityRemoveException(e.getMessage());
                }
            }
        };
        compositeQueueAdapter.init();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startCompositeQueues done");
    }

    private void stopCompositeQueues() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "stopCompositeQueues ...");
        compositeQueueAdapter.close();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "stopCompositeQueues done");
    }

    /**
     * Startup the swiftlet. Check if all required properties are defined and all other
     * startup conditions are met. Do startup work (i. e. start working thread, get/open resources).
     * If any condition prevends from startup fire a SwiftletException.
     *
     * @throws com.swiftmq.swiftlet.SwiftletException
     */
    protected void startup(Configuration config)
            throws SwiftletException {
        startupTime = System.currentTimeMillis();
        ctx = createSwiftletContext(config);

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup ...");

        ctx.usageList.getCommandRegistry().addCommand(new Viewer(ctx).createCommand());
        ctx.usageList.getCommandRegistry().addCommand(new Exporter(ctx).createCommand());
        ctx.usageList.getCommandRegistry().addCommand(new Importer(ctx).createCommand());
        ctx.usageList.getCommandRegistry().addCommand(new Remover(ctx).createCommand());
        ctx.usageList.getCommandRegistry().addCommand(new Copier(ctx).createCommand());
        ctx.usageList.getCommandRegistry().addCommand(new Mover(ctx).createCommand());
        ctx.usageList.getCommandRegistry().addCommand(new Resetter(ctx).createCommand());
        if (ctx.smartTree)
            ctx.usageList.getTemplate().removeEntities();

        Property prop = ctx.root.getProperty(PROP_MAX_FLOWCONTROL_DELAY);
        maxFlowControlDelay = ((Long) prop.getValue()).longValue();

        queueControllerList = (EntityList) ctx.root.getEntity("queue-controllers");
        ctx.messageQueueFactory = createMessageQueueFactory();
        ctx.messageGroupDispatchPolicyFactory = createMessaageGroupDispatchPolicyFactory();
        ctx.cacheTableFactory = createCacheTableFactory();
        ctx.dispatchPolicyRegistry = new DispatchPolicyRegistry(ctx);
        ctx.redispatcherController = new RedispatcherController(ctx);
        regularQueueFactory = createRegularQueueFactory();
        tempQueueFactory = createTempQueueFactory();
        systemQueueFactory = createSystemQueueFactory();
        tempQueueController = getQueueController(PREFIX_TEMP_QUEUE + "000");
        if (tempQueueController == null)
            throw new SwiftletException("No Queue Controller for temporary Queues defined!");
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "Queue Controller for temp queues: " + tempQueueController.getName());

        localRouterName = SwiftletManager.getInstance().getRouterName();
        queueTable = new HashMap();
        inboundRedirectors = new HashMap();
        outboundRedirectors = new HashMap();

        // Create DLQ
        try {
            createQueue(DLQ, (ActiveLogin) null);
            ((MessageQueue) getQueueForInternalUse(DLQ)).setAlwaysDeliverExpired(true);
        } catch (Exception e) {
            e.printStackTrace();
            throw new SwiftletException(e.toString());
        }
        createQueues((EntityList) ctx.root.getEntity("queues"));

        SwiftletManager.getInstance().addSwiftletManagerListener("sys$jndi", new SwiftletManagerAdapter() {
            public void swiftletStarted(SwiftletManagerEvent evt) {
                try {
                    ctx.jndiSwiftlet = (JNDISwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$jndi");
                    ctx.jndiAliasList = (EntityList) SwiftletManager.getInstance().getConfiguration("sys$jndi").getEntity("aliases");
                    registerJNDIQueues();
                } catch (Exception e) {
                    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "swiftletStartet, exception=" + e);
                }
            }

            public void swiftletStopped(SwiftletManagerEvent swiftletManagerEvent) {
                ctx.jndiSwiftlet = null;
                ctx.jndiAliasList = null;
            }
        });
        SwiftletManager.getInstance().addSwiftletManagerListener("sys$mgmt", new SwiftletManagerAdapter() {
            public void swiftletStarted(SwiftletManagerEvent evt) {
                try {
                    ctx.mgmtSwiftlet = (MgmtSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$mgmt");
                    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "registering MgmtListener ...");
                    ctx.mgmtSwiftlet.addMgmtListener(new MgmtListener() {
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
                    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "swiftletStartet, exception=" + e);
                }
            }
        });

        prop = ctx.root.getProperty(PROP_LOG_DUPLICATES);
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                logDuplicates = ((Boolean) newValue).booleanValue();
            }
        });
        logDuplicates = ((Boolean) prop.getValue()).booleanValue();
        prop = ctx.root.getProperty(PROP_LOG_EXPIRED);
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                logExpired = ((Boolean) newValue).booleanValue();
            }
        });
        logExpired = ((Boolean) prop.getValue()).booleanValue();
        prop = ctx.root.getProperty(PROP_DELIVER_EXPIRED);
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                deliverExpired = ((Boolean) newValue).booleanValue();
            }
        });
        deliverExpired = ((Boolean) prop.getValue()).booleanValue();
        prop = ctx.root.getProperty(PROP_COLLECT_INTERVAL);
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
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "startup: registering message count collector");
                ctx.timerSwiftlet.addTimerListener(collectInterval, this);
            } else if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "startup: collect interval <= 0; no message count collector");
        }
        prop = ctx.root.getProperty(PROP_MULTI_QUEUE_TX_GLOBAL_LOCK);
        if (prop != null) {
            prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
                public void propertyChanged(Property property, Object oldValue, Object newValue)
                        throws PropertyChangeException {
                    setUseGlobalLocking(((Boolean) newValue).booleanValue());
                }
            });
            setUseGlobalLocking(((Boolean) prop.getValue()).booleanValue());
        }

        SwiftletManager.getInstance().addSwiftletManagerListener("sys$scheduler", new SwiftletManagerAdapter() {
            public void swiftletStarted(SwiftletManagerEvent event) {
                ctx.schedulerSwiftlet = (SchedulerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$scheduler");
                jobRegistrar = new JobRegistrar(ctx);
                jobRegistrar.register();
            }

            public void swiftletStopInitiated(SwiftletManagerEvent event) {
                jobRegistrar.unregister();
            }
        });

        SwiftletManager.getInstance().addSwiftletManagerListener("sys$routing", new SwiftletManagerAdapter() {
            public void swiftletStarted(SwiftletManagerEvent event) {
                try {
                    startCluster();
                    startCompositeQueues();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                ctx.routingSwiftlet = (RoutingSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$routing");
                routingListener = new ClusterRoutingListener();
                ctx.routingSwiftlet.addRoutingListener(routingListener);
            }

            public void swiftletStopInitiated(SwiftletManagerEvent event) {
                ctx.routingSwiftlet.removeRoutingListener(routingListener);
                try {
                    stopCluster();
                    stopCompositeQueues();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup: done.");
    }

    /**
     * Shutdown the swiftlet. Check if all shutdown conditions are met. Do shutdown work (i. e. stop working thread, close resources).
     * If any condition prevends from shutdown fire a SwiftletException.
     *
     * @throws com.swiftmq.swiftlet.SwiftletException
     */
    protected void shutdown()
            throws SwiftletException {
        // true if shutdown while standby
        if (ctx == null)
            return;

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown ...");

        if (collectOn && collectInterval > 0)
            ctx.timerSwiftlet.removeTimerListener(this);

        synchronized (qSemaphore) {
            Set queues = queueTable.keySet();
            Iterator iter = queues.iterator();
            while (iter.hasNext()) {
                ActiveQueue queue = (ActiveQueue) queueTable.get(iter.next());
                if (queue.getAbstractQueue().isRunning()) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "shutdown: stopping queue '" + queue.getAbstractQueue().getQueueName() + "'");
                    try {
                        stopQueue(queue);
                    } catch (Exception ignored) {
                    }
                }
            }
            queueTable.clear();
            inboundRedirectors.clear();
            outboundRedirectors.clear();
        }
        listeners.clear();
        allQueueListeners.clear();
        regularQueueFactory = null;
        tempQueueFactory = null;
        systemQueueFactory = null;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown: done.");
        ctx = null;
    }

    private class CleanupListener implements PropertyWatchListener {
        ActiveQueueImpl queue = null;
        String queueName = null;

        public CleanupListener(ActiveQueueImpl queue) {
            this.queue = queue;
            queueName = queue.getAbstractQueue().getQueueName();
        }

        public void propertyValueChanged(Property property) {
            long newInterval = ((Long) property.getValue()).longValue();
            long oldInterval = 0;
            try {
                oldInterval = queue.getAbstractQueue().getCleanUpInterval();
            } catch (Exception ignored) {
            }
            queue.getAbstractQueue().setCleanUpInterval(newInterval);
            if (oldInterval > 0) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "propertyValueChanged: queue '" + queueName + "' removeTimerListener for interval " + oldInterval);
                ctx.timerSwiftlet.removeTimerListener(queue);
            }
            if (newInterval > 0) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "propertyValueChanged: queue '" + queueName + "' addTimerListener for interval " + newInterval);
                ctx.timerSwiftlet.addTimerListener(newInterval, queue);
            }
        }
    }

    private class ClusterRoutingListener extends RoutingListenerAdapter {
        public void destinationDeactivated(RoutingEvent routingEvent) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "destinationDeactivated: " + routingEvent.getDestination());
            ctx.dispatchPolicyRegistry.removeRouterMetrics(routingEvent.getDestination());
        }
    }
}



