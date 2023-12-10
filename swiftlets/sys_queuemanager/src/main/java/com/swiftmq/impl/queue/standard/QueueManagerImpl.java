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
import com.swiftmq.tools.concurrent.AtomicWrappingCounterLong;
import com.swiftmq.tools.sql.LikeComparator;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.versioning.Versionable;
import com.swiftmq.tools.versioning.Versioned;
import com.swiftmq.util.SwiftUtilities;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
    static final String PREFIX_TEMP_QUEUE = "tmp$";
    static final char SYSTEM_QUEUE_CHAR = '$';

    SwiftletContext ctx = null;
    EntityList queueControllerList = null;
    Entity tempQueueController = null;
    EntityListEventAdapter clusteredQueueAdapter = null;
    EntityListEventAdapter compositeQueueAdapter = null;
    ClusterRoutingListener routingListener = null;

    Map<String, ActiveQueue> queueTable = null;
    Map<String, String> inboundRedirectors = null;
    Map<String, String> outboundRedirectors = null;

    JobRegistrar jobRegistrar = null;

    Map<String, Set<QueueManagerListener>> listeners = new ConcurrentHashMap<>();
    Set<QueueManagerListener> allQueueListeners = ConcurrentHashMap.newKeySet();
    QueueFactory regularQueueFactory = null;
    QueueFactory tempQueueFactory = null;
    QueueFactory systemQueueFactory = null;
    final AtomicBoolean collectOn = new AtomicBoolean(false);
    final AtomicLong collectInterval = new AtomicLong(-1);
    final AtomicBoolean logExpired = new AtomicBoolean(true);
    final AtomicBoolean logDuplicates = new AtomicBoolean(true);
    final AtomicBoolean deliverExpired = new AtomicBoolean(true);
    final AtomicLong maxFlowControlDelay = new AtomicLong(5000);
    final AtomicLong startupTime = new AtomicLong();

    String localRouterName = null;

    AtomicWrappingCounterLong tmpCount = new AtomicWrappingCounterLong(0);
    AtomicWrappingCounterLong browserId = new AtomicWrappingCounterLong(0);
    AtomicWrappingCounterLong senderId = new AtomicWrappingCounterLong(0);
    AtomicWrappingCounterLong receiverId = new AtomicWrappingCounterLong(0);


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
        if (map != null && !map.isEmpty()) {
            for (Object o : map.entrySet()) {
                Entity e = (Entity) ((Map.Entry<?, ?>) o).getValue();
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
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createTemporaryQueueName, cnt= " + tmpCount.get());
        return PREFIX_TEMP_QUEUE + tmpCount.getAndIncrement() + '-' + startupTime.get();
    }

    public long getMaxFlowControlDelay() {
        return maxFlowControlDelay.get();
    }

    public boolean isLogDuplicates() {
        return logDuplicates.get();
    }

    public boolean isLogExpired() {
        return logExpired.get();
    }

    public boolean isDeliverExpired() {
        return deliverExpired.get();
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

    private ActiveQueue getQueue(String queueName)
            throws QueueException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "getQueue '" + queueName + "'");
        ActiveQueue queue = queueTable.get(queueName);
        if (queue == null)
            throw new UnknownQueueException("queue '" + queueName + "' is unknown");
        return queue;
    }

    private ActiveQueue getOrCreateQueue(String queueName, Entity queueEntity, QueueFactory queueFactory) throws QueueException {
        AtomicReference<QueueException> exception = new AtomicReference<>();
        ActiveQueue queue = queueTable.computeIfAbsent(fqn(queueName), key -> {
            String localName = stripLocalName(key);
            Entity queueController;
            ActiveQueue newQueue = null;
            boolean createUsage;

            try {
                if (queueFactory != null) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "getOrCreateQueue: Queue '" + localName + "' creating queue from " + queueFactory);
                    if (queueEntity != null)
                        newQueue = new ActiveQueueImpl(queueFactory.createQueue(localName, queueEntity), queueEntity);
                    else {
                        Entity queEntity = getQueueEntity(localName);
                        if (queEntity != null)
                            newQueue = new ActiveQueueImpl(queueFactory.createQueue(localName, getQueueEntity(localName)), null);
                        else {
                            queueController = getQueueController(localName);
                            if (queueController == null)
                                throw new QueueException("No matching Queue Controller found for queue: " + localName);
                            newQueue = new ActiveQueueImpl(queueFactory.createQueue(localName, queueController), queueController);
                        }
                    }
                    createUsage = queueFactory.registerUsage();
                } else if (isTemporaryQueue(localName)) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "getOrCreateQueue: Queue '" + localName + "' creating queue from tempQueueFactory");
                    newQueue = new ActiveQueueImpl(tempQueueFactory.createQueue(localName, tempQueueController), tempQueueController);
                    newQueue.getAbstractQueue().setTemporary(true);
                    createUsage = tempQueueFactory.registerUsage();
                } else if (isSystemQueue(localName)) {
                    queueController = getQueueController(localName);
                    if (queueController == null)
                        throw new QueueException("No matching Queue Controller found for queue: " + localName);
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "getOrCreateQueue: Queue '" + localName + "' creating queue from systemQueueFactory, using Queue Controller: " + queueController.getName());
                    newQueue = new ActiveQueueImpl(systemQueueFactory.createQueue(localName, queueController), queueController);
                    createUsage = systemQueueFactory.registerUsage();
                } else {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "getOrCreateQueue: Queue '" + localName + "' creating queue from regularQueueFactory");
                    newQueue = new ActiveQueueImpl(regularQueueFactory.createQueue(localName, queueEntity), queueEntity);
                    createUsage = regularQueueFactory.registerUsage();
                }
                startQueueAndCreateUsage(key, newQueue, localName, createUsage);
            } catch (Exception e) {
                e.printStackTrace();
                exception.set(new QueueException("Error creating queue: " + e.getMessage()));
            }

            return newQueue;
        });

        if (exception.get() != null) {
            throw exception.get();
        }
        return queue;
    }

    private void startQueueAndCreateUsage(String queueName, ActiveQueue newQueue, String localName, boolean createUsage) throws QueueException, EntityAddException {
        newQueue.getAbstractQueue().setQueueName(queueName);
        newQueue.getAbstractQueue().setLocalName(localName);
        startQueue(newQueue);
        if (createUsage) {
            Entity qEntity = ctx.usageList.createEntity();
            qEntity.setName(localName);
            qEntity.setDynamicObject(newQueue);
            qEntity.createCommands();
            ctx.usageList.addEntity(qEntity);
        }
    }

    public AbstractQueue getQueueForInternalUse(String queueName) {
        return getQueueForInternalUse(queueName, false);
    }

    public AbstractQueue getQueueForInternalUse(String queueName, boolean respectRedirection) {
        queueName = fqn(queueName);
        if (respectRedirection)
            queueName = getRedirectedQueueName(outboundRedirectors, queueName);
        ActiveQueue queue = queueTable.get(queueName);
        return queue == null ? null : queue.getAbstractQueue();
    }

    public String fqn(String queueName) {
        if (queueName.indexOf('@') == -1)
            queueName += '@' + localRouterName;
        return queueName;
    }

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

    private void startQueue(ActiveQueue queue)
            throws QueueException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "startQueue: starting queue '" + queue.getAbstractQueue().getQueueName() + "'");
        fireQueueManagerEvent(queue.getAbstractQueue().getQueueName(), QueueManagerListener::queueStartInitiated, new QueueManagerEvent(this, queue.getAbstractQueue().getQueueName()));
        queue.getAbstractQueue().startQueue();
        queue.setStartupTime(System.currentTimeMillis());
        Entity queueEntity = ((ActiveQueueImpl) queue).getQueueEntity();
        if (queueEntity != null) {
            Property prop = queueEntity.getProperty(PROP_CLEANUP_INTERVAL);
            if (prop != null) {
                CleanupListener l = new CleanupListener((ActiveQueueImpl) queue);
                prop.addPropertyWatchListener(l);
                ((ActiveQueueImpl) queue).setWatchListener(l);
            }
        }
        long interval = queue.getAbstractQueue().getCleanUpInterval();
        if (interval > 0) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "startQueue: queue '" + queue.getAbstractQueue().getQueueName() + "' addTimerListener for interval " + interval);
            ctx.timerSwiftlet.addTimerListener(interval, queue);
        }
        fireQueueManagerEvent(queue.getAbstractQueue().getQueueName(), QueueManagerListener::queueStarted, new QueueManagerEvent(this, queue.getAbstractQueue().getQueueName()));
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

    private void stopQueue(ActiveQueue queue)
            throws QueueException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "stopQueue: stopping queue '" + queue.getAbstractQueue().getQueueName() + "'");
        fireQueueManagerEvent(queue.getAbstractQueue().getQueueName(), QueueManagerListener::queueStopInitiated, new QueueManagerEvent(this, queue.getAbstractQueue().getQueueName()));
        long interval = queue.getAbstractQueue().getCleanUpInterval();
        if (interval > 0) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "stopQueue: queue '" + queue.getAbstractQueue().getQueueName() + "' removeTimerListener for interval " + interval);
            ctx.timerSwiftlet.removeTimerListener(queue);
        }
        Entity queueEntity = ((ActiveQueueImpl) queue).getQueueEntity();
        if (queueEntity != null) {
            Property prop = queueEntity.getProperty(PROP_CLEANUP_INTERVAL);
            if (prop != null)
                prop.removePropertyWatchListener(((ActiveQueueImpl) queue).getWatchListener());
        }
        queue.getAbstractQueue().stopQueue();
        queue.setStartupTime(-1);
        fireQueueManagerEvent(queue.getAbstractQueue().getQueueName(), QueueManagerListener::queueStopped, new QueueManagerEvent(this, queue.getAbstractQueue().getQueueName()));
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
        for (Object o : queueTable.entrySet()) {
            ActiveQueue activeQueue = (ActiveQueue) ((Map.Entry<?, ?>) o).getValue();
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

    public boolean isQueueDefined(String queueName) {
        return queueTable.containsKey(fqn(queueName));
    }

    public boolean isQueueRunning(String queueName) {
        ActiveQueue queue = queueTable.get(getRedirectedQueueName(outboundRedirectors, fqn(queueName)));
        return queue != null && queue.getAbstractQueue().isRunning();
    }

    public QueueSender createQueueSender(String queueName, ActiveLogin activeLogin)
            throws QueueException, AuthenticationException {
        queueName = fqn(queueName);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "createQueueSender '" + queueName + "', activeLogin: " + activeLogin);
        ActiveQueue queue = null;
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
        long id = senderId.getAndIncrement();
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

    public QueueReceiver createQueueReceiver(String queueName, ActiveLogin activeLogin, Selector selector)
            throws QueueException, AuthenticationException {
        queueName = fqn(queueName);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "createQueueReceiver '" + queueName + "', activeLogin: " + activeLogin);
        ActiveQueue queue = null;
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
        long id = receiverId.getAndIncrement();
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

    public QueueBrowser createQueueBrowser(String queueName, ActiveLogin activeLogin, Selector selector)
            throws QueueException, AuthenticationException {
        queueName = fqn(queueName);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "createQueueBrowser '" + queueName + "', activeLogin: " + activeLogin + ", selector: " + selector);
        queueName = getRedirectedQueueName(inboundRedirectors, queueName);
        ActiveQueue queue = null;
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
        long id = browserId.getAndIncrement();
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
        allQueueListeners.add(l);
    }

    public void addQueueManagerListener(String queueName, QueueManagerListener l)
            throws UnknownQueueException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "addQueueManagerListener '" + queueName + "'");
        if (!isQueueDefined(queueName)) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "addQueueManagerListener: Queue '" + queueName + "' is unknown");
            throw new UnknownQueueException("queue '" + queueName + "' is unknown");
        }
        listeners.compute(queueName, (key, qListeners) -> {
            Set<QueueManagerListener> ql = qListeners;
            if (ql == null)
                ql = ConcurrentHashMap.newKeySet();
            ql.add(l);
            return ql;
        });
    }

    public void removeQueueManagerListener(QueueManagerListener l) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "removeQueueManagerListener for all queues");
        allQueueListeners.remove(l);
    }

    public void removeQueueManagerListener(String queueName, QueueManagerListener l) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "removeQueueManagerListener '" + queueName + "'");
        listeners.compute(queueName, (key, qListeners) -> {
            if (qListeners != null) {
                qListeners.remove(l);
                if (qListeners.isEmpty()) {
                    return null;
                }
            }
            return qListeners;
        });
    }

    private void fireQueueManagerEvent(String queueName, QueueManagerEventAction action, QueueManagerEvent evt) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "fireQueueManagerEvent '" + queueName + "'");

        List<QueueManagerListener> myListeners = new ArrayList<>(allQueueListeners);
        Set<QueueManagerListener> qListeners = listeners.get(queueName);
        if (qListeners != null) {
            myListeners.addAll(qListeners);
        }

        for (QueueManagerListener l : myListeners) {
            action.execute(l, evt);
        }
    }

    private void createQueue(Entity queueEntity) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createQueue: queue '" + queueEntity.getName());
        SwiftUtilities.verifyLocalQueueName(queueEntity.getName());
        getOrCreateQueue(queueEntity.getName(), queueEntity, null);
    }

    private void createQueues(EntityList queueList) throws SwiftletException {
        Map m = queueList.getEntities();
        if (!m.isEmpty()) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createQueues: begin starting queues ...");
            try {
                for (Object o : m.entrySet()) {
                    Entity queueEntity = (Entity) ((Map.Entry<?, ?>) o).getValue();
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
        for (Object o : queueTable.entrySet()) {
            ActiveQueue queue = (ActiveQueue) ((Map.Entry<?, ?>) o).getValue();
            if (queue != null) {
                AbstractQueue ac = queue.getAbstractQueue();
                if (ac != null && ac.isRunning()) {
                    String queueName = ac.getLocalName();
                    Entity queueEntity = ctx.usageList.getEntity(queueName);
                    if (queueEntity != null) {
                        try {
                            Property prop = queueEntity.getProperty(PROP_MESSAGECOUNT);
                            int oldValue = (Integer) prop.getValue();
                            int actValue = (int) ac.getNumberQueueMessages();
                            if (oldValue != actValue) {
                                prop.setValue(actValue);
                            }
                            prop = queueEntity.getProperty(PROP_MSG_CONSUME_RATE);
                            int oldCR = (Integer) prop.getValue();
                            int actCR = ac.getConsumingRate();
                            if (oldCR != actCR) {
                                prop.setValue(actCR);
                            }
                            prop = queueEntity.getProperty(PROP_MSG_PRODUCE_RATE);
                            int oldPR = (Integer) prop.getValue();
                            int actPR = ac.getProducingRate();
                            if (oldPR != actPR) {
                                prop.setValue(actPR);
                            }
                            prop = queueEntity.getProperty(PROP_TOTAL_CONSUMED);
                            int oldTC = (Integer) prop.getValue();
                            int actTC = ac.getConsumedTotal();
                            if (oldTC != actTC) {
                                prop.setValue(actTC);
                            }
                            prop = queueEntity.getProperty(PROP_TOTAL_PRODUCED);
                            int oldTP = (Integer) prop.getValue();
                            int actTP = ac.getProducedTotal();
                            if (oldTP != actTP) {
                                prop.setValue(actTP);
                            }
                            prop = queueEntity.getProperty(PROP_LATENCY);
                            long oldLT = (Long) prop.getValue();
                            long actLT = ac.getAndResetAverageLatency();
                            if (oldLT != actLT) {
                                prop.setValue(actLT);
                            }
                            prop = queueEntity.getProperty(PROP_MCACHE_MESSAGES);
                            int oldCM = (Integer) prop.getValue();
                            int actCM = ac.getCurrentCacheSizeMessages();
                            if (oldCM != actCM) {
                                prop.setValue(actCM);
                            }
                            prop = queueEntity.getProperty(PROP_MCACHE_SIZE_KB);
                            int oldS = (Integer) prop.getValue();
                            int actS = ac.getCurrentCacheSizeKB();
                            if (oldS != actS) {
                                prop.setValue(actS);
                            }
                            prop = queueEntity.getProperty(PROP_FLOWCONTROL_DELAY);
                            long oldDelay = (Long) prop.getValue();
                            long actDelay = 0;
                            FlowController fc = ac.getFlowController();
                            if (fc != null)
                                actDelay = fc.getLastDelay();
                            if (oldDelay != actValue) {
                                prop.setValue(actDelay);
                            }
                            prop = queueEntity.getProperty(PROP_AMESSAGES_MAXIMUM);
                            int oldMax = (Integer) prop.getValue();
                            int actMax = ac.getMaxMessages();
                            if (oldMax != actMax) {
                                prop.setValue(actMax);
                            }
                            prop = queueEntity.getProperty(PROP_ACACHE_SIZE);
                            int oldSize = (Integer) prop.getValue();
                            int actSize = ac.getCacheSize();
                            if (oldSize != actSize) {
                                prop.setValue(actSize);
                            }
                            prop = queueEntity.getProperty(PROP_ACACHE_SIZE_KB);
                            int oldSizeKB = (Integer) prop.getValue();
                            int actSizeKB = ac.getCacheSizeKB();
                            if (oldSizeKB != actSizeKB) {
                                prop.setValue(actSizeKB);
                            }
                            prop = queueEntity.getProperty(PROP_AFLOWCONTROL_QUEUE_SIZE);
                            int oldFC = (Integer) prop.getValue();
                            int actFC = 0;
                            fc = ac.getFlowController();
                            if (fc != null)
                                actFC = fc.getStartQueueSize();
                            if (oldFC != actFC) {
                                prop.setValue(actFC);
                            }
                            prop = queueEntity.getProperty(PROP_ACLEANUP_INTERVAL);
                            long oldCU = (Long) prop.getValue();
                            long actCU = ac.getCleanUpInterval();
                            if (oldCU != actCU) {
                                prop.setValue(actCU);
                            }
                        } catch (Exception ignored) {
                        }
                    }
                }
            }
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "collecting message counts...DONE.");
    }

    public void createQueue(String queueName, ActiveLogin activeLogin)
            throws QueueException, AuthenticationException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createQueue '" + queueName + "'");
        if (isQueueDefined(queueName))
            throw new QueueAlreadyDefinedException("queue '" + queueName + "' is already defined!");
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
            getOrCreateQueue(queueName, null, null);
        } else
            getOrCreateQueue(queueName, null, systemQueueFactory);

    }

    private void createQueue(String queueName, Entity queueEntity, QueueFactory factory)
            throws QueueException {
        if (isQueueDefined(queueName))
            throw new QueueAlreadyDefinedException("queue '" + queueName + "' is already defined");
        getOrCreateQueue(fqn(queueName), queueEntity, factory);
    }

    public void createQueue(String queueName, QueueFactory factory)
            throws QueueException {
        getOrCreateQueue(queueName, null, factory);
    }

    public void deleteQueue(String queueName, boolean onEmpty)
            throws QueueException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "deleteQueue '" + queueName + "'");

        AtomicReference<QueueException> exception = new AtomicReference<>();
        queueTable.computeIfPresent(fqn(queueName), (key, queue) -> {
            try {
                if (!onEmpty || queue.getAbstractQueue().getNumberQueueMessages() == 0) {
                    queue.getAbstractQueue().deleteContent();
                    if (queue.getAbstractQueue().isRunning()) {
                        stopQueue(queue);
                    }
                    listeners.remove(key);
                    ctx.usageList.removeDynamicEntity(queue);
                    return null; // Returning null will remove the entry from queueTable
                }
            } catch (Exception e) {
                exception.set(new QueueException("Error deleting queue: " + e.getMessage()));
            }
            return queue; // Return the queue if it's not deleted
        });
        if (exception.get() != null)
            throw exception.get();
    }

    public String createTemporaryQueue() throws QueueException {
        String queueName = createTemporaryQueueName();
        String tmpQueueName = fqn(queueName);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createTemporaryQueue '" + tmpQueueName + "'");
        getOrCreateQueue(queueName, null, null);
        return tmpQueueName;
    }

    public void deleteTemporaryQueue(String queueName) throws QueueException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "deleteTemporaryQueue '" + queueName + "'");

        queueTable.computeIfPresent(fqn(queueName), (key, queue) -> {
            if (queue.getAbstractQueue().isRunning()) {
                try {
                    stopQueue(queue);
                } catch (QueueException e) {
                    throw new RuntimeException(e);
                }
            }
            // Additional removal logic
            listeners.remove(queueName);
            ctx.usageList.removeDynamicEntity(queue);
            return null;
        });
    }

    public void purgeQueue(String queueName)
            throws QueueException {
        queueName = fqn(queueName);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "purgeQueue '" + queueName + "'");
        ActiveQueue queue = getQueue(queueName);
        if (queue.getAbstractQueue().isRunning()) {
            queue.getAbstractQueue().deleteContent();
        }
    }

    public String[] getDefinedQueueNames() {
        return queueTable.keySet().toArray(String[]::new);
    }

    private String getRedirectedQueueName(Map<String, String> redirectorTable, String queueName) {
        Optional<String> redirectedName = redirectorTable.keySet().stream()
                .filter(predicate -> LikeComparator.compare(queueName, predicate, '\\'))
                .findFirst()
                .map(redirectorTable::get);

        String name = redirectedName.orElse(queueName);

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "getRedirectedQueueName '" + queueName + "' returns '" + name + "'");

        return name;
    }
    public void setQueueOutboundRedirector(String likePredicate, String outboundQueueName)
            throws UnknownQueueException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "setQueueOutboundRedirector, likePredicate '" + likePredicate + "' outboundQueueName '" + outboundQueueName + "'");
        if (outboundQueueName == null)
            outboundRedirectors.remove(likePredicate);
        else {
            if (!isQueueDefined(outboundQueueName))
                throw new UnknownQueueException("queue '" + outboundQueueName + "' is unknown!");
            outboundRedirectors.put(likePredicate, outboundQueueName);
        }
    }

    public void setQueueInboundRedirector(String likePredicate, String inboundQueueName)
            throws UnknownQueueException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "setQueueInboundRedirector, likePredicate '" + likePredicate + "' inboundQueueName '" + inboundQueueName + "'");
        if (inboundQueueName == null)
            inboundRedirectors.remove(likePredicate);
        else {
            if (!isQueueDefined(inboundQueueName))
                throw new UnknownQueueException("queue '" + inboundQueueName + "' is unknown!");
            inboundRedirectors.put(likePredicate, inboundQueueName);
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
                try {
                    String qn = fqn(oldEntity.getName());
                    if (isQueueDefined(qn)) {
                        AbstractQueue queue = getQueueForInternalUse(qn);
                        queue.setQueueReceiverListener(null);
                        String clusteredQueueName = parent.getParent().getName();
                        DispatchPolicy dp = ctx.dispatchPolicyRegistry.get(clusteredQueueName);
                        dp.removeLocalMetric((QueueMetric) oldEntity.getUserObject());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
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

    protected void startup(Configuration config)
            throws SwiftletException {
        startupTime.set(System.currentTimeMillis());
        ctx = createSwiftletContext(config);

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup ...");

        ctx.usageList.getCommandRegistry().addCommand(new Activate(ctx).createCommand());
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
        maxFlowControlDelay.set(((Long) prop.getValue()).longValue());

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
        queueTable = new ConcurrentHashMap<>();
        inboundRedirectors = new ConcurrentHashMap<>();
        outboundRedirectors = new ConcurrentHashMap<>();

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

        prop = ctx.root.getProperty(PROP_LOG_DUPLICATES);
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                logDuplicates.set((Boolean) newValue);
            }
        });
        logDuplicates.set((Boolean) prop.getValue());
        prop = ctx.root.getProperty(PROP_LOG_EXPIRED);
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                logExpired.set((Boolean) newValue);
            }
        });
        logExpired.set(((Boolean) prop.getValue()).booleanValue());
        prop = ctx.root.getProperty(PROP_DELIVER_EXPIRED);
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                deliverExpired.set((Boolean) newValue);
            }
        });
        deliverExpired.set((Boolean) prop.getValue());
        prop = ctx.root.getProperty(PROP_COLLECT_INTERVAL);
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
                    ctx.traceSpace.trace(getName(), "startup: registering message count collector");
                ctx.timerSwiftlet.addTimerListener(collectInterval.get(), this);
            } else if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "startup: collect interval <= 0; no message count collector");
        }
        prop = ctx.root.getProperty(PROP_MULTI_QUEUE_TX_GLOBAL_LOCK);
        if (prop != null) {
            prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
                public void propertyChanged(Property property, Object oldValue, Object newValue)
                        throws PropertyChangeException {
                    setUseGlobalLocking((Boolean) newValue);
                }
            });
            setUseGlobalLocking((Boolean) prop.getValue());
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

    protected void shutdown()
            throws SwiftletException {
        // true if shutdown while standby
        if (ctx == null)
            return;

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown ...");

        if (collectOn.get() && collectInterval.get() > 0)
            ctx.timerSwiftlet.removeTimerListener(this);

        for (String queueName : queueTable.keySet()) {
            ActiveQueue queue = queueTable.get(queueName);
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
            long newInterval = (Long) property.getValue();
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

    @FunctionalInterface
    private interface QueueManagerEventAction {
        void execute(QueueManagerListener listener, QueueManagerEvent evt);
    }

}



