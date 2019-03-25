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

package com.swiftmq.impl.topic.standard;

import com.swiftmq.impl.topic.standard.announce.TopicAnnounceReceiver;
import com.swiftmq.impl.topic.standard.announce.TopicAnnounceSender;
import com.swiftmq.impl.topic.standard.announce.TopicInfo;
import com.swiftmq.impl.topic.standard.jobs.JobRegistrar;
import com.swiftmq.jms.DestinationFactory;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.mgmt.*;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.auth.AuthenticationException;
import com.swiftmq.swiftlet.auth.AuthenticationSwiftlet;
import com.swiftmq.swiftlet.event.SwiftletManagerAdapter;
import com.swiftmq.swiftlet.event.SwiftletManagerEvent;
import com.swiftmq.swiftlet.jndi.JNDISwiftlet;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.swiftlet.routing.Route;
import com.swiftmq.swiftlet.routing.RoutingSwiftlet;
import com.swiftmq.swiftlet.scheduler.SchedulerSwiftlet;
import com.swiftmq.swiftlet.store.DurableStoreEntry;
import com.swiftmq.swiftlet.store.DurableSubscriberStore;
import com.swiftmq.swiftlet.store.StoreSwiftlet;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.topic.TopicException;
import com.swiftmq.swiftlet.topic.TopicManager;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;
import com.swiftmq.tools.collection.ArrayListTool;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.versioning.Versionable;
import com.swiftmq.tools.versioning.Versioned;
import com.swiftmq.util.SwiftUtilities;

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import java.io.IOException;
import java.util.*;

public class TopicManagerImpl extends TopicManager
        implements QueueFactory {
    public final static String TOPIC_PREFIX = "tpc$";
    public final static String TOPIC_DELIMITER = ".";
    public final static char TOPIC_DELIMITER_CHAR = '.';
    public final static String DURABLE_DELIMITER = "$";
    public final static String DURABLE_TYPE = "DURABLE";

    // Routing-Queue
    public static final String TOPIC_QUEUE = "sys$topic";

    Configuration config = null;
    Entity root = null;
    TopicManagerContext ctx = null;

    HashMap rootBrokers = new HashMap();
    ArrayList topicSubscriptions = new ArrayList();
    HashMap durableSubscriptions = new HashMap();
    DurableSubscriberStore durableStore = null;

    String topicQueue = null;
    boolean flowControlEnabled = true;
    boolean programmaticDurableInProgress = false;
    JobRegistrar jobRegistrar = null;
    boolean directSubscriberSelection = true;

    protected String getTopicQueuePrefix() {
        return TOPIC_PREFIX;
    }

    protected String getTopicDelimiter() {
        return TOPIC_DELIMITER;
    }

    public boolean isDirectSubscriberSelection() {
        return directSubscriberSelection;
    }

    public AbstractQueue createQueue(String queueName, Entity queueEntity)
            throws QueueException {
        StringTokenizer t = new StringTokenizer(queueName, "$@");
        t.nextToken(); // the TOPIC_PREFIX
        String rootTopicName = t.nextToken();
        TopicBroker topicBroker = new TopicBroker(ctx, rootTopicName);
        rootBrokers.put(TOPIC_PREFIX + rootTopicName, topicBroker);
        if (flowControlEnabled)
            ((AbstractQueue) topicBroker).setFlowController(new TopicFlowController());
        return topicBroker;
    }

    private void registerJNDI(String name, TopicImpl topic) {
        try {
            DataByteArrayOutputStream dos = new DataByteArrayOutputStream();
            DestinationFactory.dumpDestination(topic, dos);
            Versionable versionable = new Versionable();
            versionable.addVersioned(-1, new Versioned(-1, dos.getBuffer(), dos.getCount()), "com.swiftmq.jms.DestinationFactory");
            ctx.jndiSwiftlet.registerJNDIObject(name, versionable);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void createTopics(EntityList topicList) throws Exception {
        Property prop = root.getProperty("flowcontrol-enabled");
        flowControlEnabled = ((Boolean) prop.getValue()).booleanValue();
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                flowControlEnabled = ((Boolean) newValue).booleanValue();
                synchronized (TopicManagerImpl.this) {
                    for (Iterator iter = rootBrokers.entrySet().iterator(); iter.hasNext(); ) {
                        TopicBroker broker = (TopicBroker) ((Map.Entry) iter.next()).getValue();
                        if (flowControlEnabled)
                            ((AbstractQueue) broker).setFlowController(new TopicFlowController());
                        else
                            ((AbstractQueue) broker).setFlowController(null);
                    }
                }
            }
        });
        prop = root.getProperty("direct-subscriber-selection");
        directSubscriberSelection = ((Boolean) prop.getValue()).booleanValue();
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                directSubscriberSelection = ((Boolean) newValue).booleanValue();
            }
        });

        Map m = topicList.getEntities();
        if (m.size() > 0) {
            for (Iterator iter = m.entrySet().iterator(); iter.hasNext(); ) {
                Entity topicEntity = (Entity) ((Map.Entry) iter.next()).getValue();
                SwiftUtilities.verifyTopicName(topicEntity.getName());
                createTopic(topicEntity.getName());
            }
        }
        topicList.setEntityAddListener(new EntityChangeAdapter(null) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                try {
                    String name = newEntity.getName();
                    SwiftUtilities.verifyTopicName(name);
                    try {
                        createTopic(name);
                    } catch (TopicException e) {
                        // Ignore as topic may already exist
                    }
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "onEntityAdd (topics): new topic=" + name);
                } catch (Exception e) {
                    throw new EntityAddException(e.getMessage());
                }
            }
        });
        topicList.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                try {
                    String name = delEntity.getName();
                    deleteTopic(name);
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "onEntityRemove (topics): del topic=" + name);
                } catch (Exception e) {
                    throw new EntityRemoveException(e.getMessage());
                }
            }
        });
    }

    private synchronized void createTopicAdministratively(String topicName) throws TopicException {
        if (isTopicDefined(topicName))
            throw new TopicException("Topic '" + topicName + "' is already defined");
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createTopic: creating topic: " + topicName);
        ctx.logSwiftlet.logInformation(getName(), "create topic: " + topicName);
        String[] tokenizedName = tokenizeTopicName(topicName, TOPIC_DELIMITER);
        String rootName = TOPIC_PREFIX + tokenizedName[0];
        TopicBroker rootBroker = (TopicBroker) rootBrokers.get(rootName);
        if (rootBroker == null) {
            try {
                ctx.queueManager.createQueue(rootName, this);
            } catch (Exception e) {
                throw new TopicException(e.getMessage());
            }
            rootBroker = (TopicBroker) rootBrokers.get(rootName);
        }
        rootBroker.addTopic(topicName, tokenizedName);
        if (ctx.jndiSwiftlet != null)
            registerJNDI(topicName, new TopicImpl(topicName));
    }

    public synchronized void createTopic(String topicName) throws TopicException {
        if (isTopicDefined(topicName))
            throw new TopicException("Topic '" + topicName + "' is already defined");
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createTopic: creating topic: " + topicName);
        ctx.logSwiftlet.logInformation(getName(), "create topic: " + topicName);
        String[] tokenizedName = tokenizeTopicName(topicName, TOPIC_DELIMITER);
        String rootName = TOPIC_PREFIX + tokenizedName[0];
        TopicBroker rootBroker = (TopicBroker) rootBrokers.get(rootName);
        if (rootBroker == null) {
            try {
                ctx.queueManager.createQueue(rootName, this);
            } catch (Exception e) {
                throw new TopicException(e.getMessage());
            }
            rootBroker = (TopicBroker) rootBrokers.get(rootName);
        }
        rootBroker.addTopic(topicName, tokenizedName);
        if (ctx.jndiSwiftlet != null)
            registerJNDI(topicName, new TopicImpl(topicName));
    }

    public synchronized void deleteTopic(String topicName) throws TopicException {
        if (!isTopicDefined(topicName))
            throw new TopicException("Topic '" + topicName + "' is unknown");
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "deleteTopic: deleting topic: " + topicName);
        ctx.logSwiftlet.logInformation(getName(), "delete topic: " + topicName);
        String[] tokenizedName = tokenizeTopicName(topicName, TOPIC_DELIMITER);
        String rootName = TOPIC_PREFIX + tokenizedName[0];
        TopicBroker rootBroker = (TopicBroker) rootBrokers.get(rootName);
        rootBroker.removeTopic(topicName, tokenizedName);
        if (ctx.jndiSwiftlet != null)
            ctx.jndiSwiftlet.deregisterJNDIObject(topicName);
    }

    public synchronized boolean isTopicDefined(String topicName) {
        String[] token = tokenizeTopicName(topicName);
        if (token == null || token.length == 0)
            return false;
        TopicBroker rootBroker = (TopicBroker) rootBrokers.get(TOPIC_PREFIX + token[0]);
        return rootBroker != null && rootBroker.getTopicName(token) != null;
    }

    public synchronized TopicImpl verifyTopic(TopicImpl topic) throws JMSException, InvalidDestinationException {
        String[] token = tokenizeTopicName(topic.getTopicName());
        String brokerQueueName = TOPIC_PREFIX + token[0];
        // check the rootBroker
        TopicBroker rootBroker = (TopicBroker) rootBrokers.get(brokerQueueName);

        if (!(rootBroker != null && rootBroker.getTopicName(token) != null)) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "verifyTopic: creating topic: " + topic.getTopicName());
            try {
                createTopic(topic.getTopicName());
            } catch (Exception ignore) {
            }
        }

        // if ok, check if queueName is defined
        String queueName = topic.getQueueName();
        if (queueName == null)
            topic.setQueueName(brokerQueueName);

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "verifyTopic: '" + topic.getTopicName() + "' ok. Topic queue = " + topic.getQueueName());

        return topic;
    }

    protected static String concatName(String[] tokenizedName) {
        StringBuffer s = new StringBuffer();
        for (int i = 0; i < tokenizedName.length; i++) {
            if (i != 0)
                s.append(TOPIC_DELIMITER);
            s.append(tokenizedName[i]);
        }
        return s.toString();
    }

    public synchronized int subscribe(TopicImpl topic, Selector selector, boolean noLocal, String queueName, ActiveLogin activeLogin)
            throws AuthenticationException {
        int subscriberId = 0;
        String brokerQueueName = null;
        String topicName = null;
        try {
            brokerQueueName = topic.getQueueName();
            topicName = topic.getTopicName();
        } catch (JMSException ignored) {
        }
        ;
        if (activeLogin != null && !activeLogin.getType().equals(DURABLE_TYPE))
            ctx.authSwiftlet.verifyTopicReceiverSubscription(topicName, activeLogin.getLoginId());
        TopicBroker rootBroker = (TopicBroker) rootBrokers.get(brokerQueueName);
        subscriberId = ArrayListTool.setFirstFreeOrExpand(topicSubscriptions, null);
        TopicSubscription subscription = new TopicSubscription(subscriberId, topicName,
                tokenizeTopicName(topicName, TOPIC_DELIMITER),
                noLocal, selector, activeLogin, queueName);
        subscription.setBroker(rootBroker);
        topicSubscriptions.set(subscriberId, subscription);
        rootBroker.subscribe(subscription);

        try {
            Entity subEntity = ctx.activeSubscriberList.createEntity();
            subEntity.setName(topicName + "-" + subscriberId);
            subEntity.setDynamicObject(subscription);
            subEntity.createCommands();
            Property prop = subEntity.getProperty("clientid");
            prop.setValue(activeLogin != null ? activeLogin.getClientId() : "Internal Swiftlet Usage");
            prop.setReadOnly(true);
            prop = subEntity.getProperty("topic");
            prop.setValue(topicName);
            prop.setReadOnly(true);
            prop = subEntity.getProperty("boundto");
            prop.setValue(queueName);
            prop.setReadOnly(true);
            prop = subEntity.getProperty("nolocal");
            prop.setValue(new Boolean(noLocal));
            prop.setReadOnly(true);
            prop = subEntity.getProperty("selector");
            if (selector != null) {
                prop.setValue(selector.getConditionString());
            }
            prop.setReadOnly(true);
            ctx.activeSubscriberList.addEntity(subEntity);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "subscribe: topicSubscription = " + subscription);
        return subscriberId;
    }

    public String subscribeDurable(String durableName, TopicImpl topic, Selector selector, boolean noLocal, ActiveLogin activeLogin)
            throws AuthenticationException, QueueException, QueueAlreadyDefinedException, UnknownQueueException, TopicException {
        return subscribeDurable(durableName, topic, selector, noLocal, activeLogin, null);
    }

    public String subscribeDurable(String durableName, TopicImpl topic, Selector selector, boolean noLocal, ActiveLogin activeLogin, Entity newEntity)
            throws AuthenticationException, QueueException, QueueAlreadyDefinedException, UnknownQueueException, TopicException {
        return subscribeDurable(durableName, topic, selector, noLocal, activeLogin, newEntity, true);
    }

    private synchronized String subscribeDurable(String durableName, TopicImpl topic, Selector selector, boolean noLocal, ActiveLogin activeLogin, Entity newEntity, boolean verifyAuth)
            throws AuthenticationException, QueueException, QueueAlreadyDefinedException, UnknownQueueException, TopicException {
        String topicName = null;
        try {
            topicName = topic.getTopicName();
        } catch (JMSException ignored) {
        }
        if (verifyAuth)
            ctx.authSwiftlet.verifyTopicDurableSubscriberCreation(topicName, activeLogin.getLoginId());
        String durableQueueName = DurableSubscription.createDurableQueueName(activeLogin.getClientId(), durableName);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "subscribeDurable: topic = " + topicName + ", durableQueueName = " + durableQueueName);
        DurableSubscription durable = (DurableSubscription) durableSubscriptions.get(durableQueueName);
        if (durable != null && durable.hasChanged(topicName, selector, noLocal)) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "subscribeDurable: durable has changed, deleting ...");
            unsubscribe(durable.getTopicSubscription().getSubscriberId());
            ctx.queueManager.purgeQueue(durableQueueName);
            ctx.queueManager.deleteQueue(durableQueueName, false);
            durableSubscriptions.remove(durableQueueName);
            try {
                durableStore.deleteDurableStoreEntry(durable.getClientId(), durable.getDurableName());
            } catch (Exception e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "subscribeDurable: error deleting durable subscription: " + e);
                throw new TopicException("error deleting durable subscription: " + e);
            }
            durable = null;
            ctx.activeDurableList.removeDynamicEntity(durable);
        }
        if (durable == null) {
            durable = new DurableSubscription(activeLogin.getClientId(), durableName, topicName, selector, noLocal);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "subscribeDurable: creating new durable = " + durable);
            durableSubscriptions.put(durableQueueName, durable);
            try {
                durableStore.insertDurableStoreEntry(durable.getDurableStoreEntry());
            } catch (Exception e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "subscribeDurable: error saving durable subscription: " + e);
                throw new TopicException("error saving durable subscription: " + e);
            }
            ctx.queueManager.createQueue(durableQueueName, (ActiveLogin) null);
            int id = subscribe(topic, selector, noLocal, durableQueueName, activeLogin);
            durable.setTopicSubscription((TopicSubscription) topicSubscriptions.get(id));

            try {
                if (newEntity == null) {
                    Entity durEntity = ctx.activeDurableList.createEntity();
                    durEntity.setName(durableQueueName);
                    durEntity.setDynamicObject(durable);
                    durEntity.createCommands();
                    Property prop = durEntity.getProperty("clientid");
                    prop.setValue(activeLogin.getClientId());
                    prop.setReadOnly(true);
                    prop = durEntity.getProperty("durablename");
                    prop.setValue(durableName);
                    prop.setReadOnly(true);
                    prop = durEntity.getProperty("topic");
                    prop.setValue(topicName);
                    prop.setReadOnly(true);
                    prop = durEntity.getProperty("boundto");
                    prop.setValue(durableQueueName);
                    prop.setReadOnly(true);
                    prop = durEntity.getProperty("nolocal");
                    prop.setValue(new Boolean(noLocal));
                    prop.setReadOnly(true);
                    prop = durEntity.getProperty("selector");
                    if (selector != null) {
                        prop.setValue(selector.getConditionString());
                    }
                    prop.setReadOnly(true);
                    programmaticDurableInProgress = true;
                    ctx.activeDurableList.addEntity(durEntity);
                    programmaticDurableInProgress = false;
                } else {
                    newEntity.setDynamicObject(durable);
                    Property prop = newEntity.getProperty("boundto");
                    prop.setValue(durableQueueName);
                    prop.setReadOnly(true);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return durableQueueName;
    }

    /**
     * @param durableName
     * @param activeLogin
     * @throws InvalidDestinationException
     */
    public synchronized void deleteDurable(String durableName, ActiveLogin activeLogin)
            throws InvalidDestinationException, QueueException, UnknownQueueException, TopicException {
        String durableQueueName = DurableSubscription.createDurableQueueName(activeLogin.getClientId(), durableName);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "deleteDurable: durableQueueName = " + durableQueueName);
        DurableSubscription durable = (DurableSubscription) durableSubscriptions.get(durableQueueName);
        if (durable == null)
            throw new InvalidDestinationException("no durable subscriber found with clientId '" + activeLogin.getClientId() + "' and name = '" + durableName + "'");

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "deleteDurable: durable found, deleting ...");
        unsubscribe(durable.getTopicSubscription().getSubscriberId());
        ctx.queueManager.deleteQueue(durableQueueName, false);
        durableSubscriptions.remove(durableQueueName);

        try {
            durableStore.deleteDurableStoreEntry(durable.getClientId(), durable.getDurableName());
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "deleteDurable: error deleting durable subscription: " + e);
            throw new TopicException("error deleting durable subscription: " + e);
        }
        ctx.activeDurableList.removeDynamicEntity(durable);
    }

    public synchronized String getDurableTopicName(String durableName, ActiveLogin activeLogin) {
        String topicName = null;
        String durableQueueName = DurableSubscription.createDurableQueueName(activeLogin.getClientId(), durableName);
        DurableSubscription durable = (DurableSubscription) durableSubscriptions.get(durableQueueName);
        if (durable != null)
            topicName = durable.getTopicName();
        return topicName;
    }

    /**
     * @param subscriberId
     */
    public synchronized void unsubscribe(int subscriberId) {
        if (subscriberId < topicSubscriptions.size()) {
            TopicSubscription subscription = (TopicSubscription) topicSubscriptions.get(subscriberId);
            if (subscription != null) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "unsubscribe: topicSubscription = " + subscription);
                subscription.unsubscribe();
                topicSubscriptions.set(subscriberId, null);
                ctx.activeSubscriberList.removeDynamicEntity(subscription);
            }
        }
    }

    public String[] getTopicNames() {
        return config.getEntity("topics").getEntityNames();
    }

    public synchronized void removeRemoteSubscriptions(String routerName) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "removeRemoteSubscriptions: " + routerName + ", removing subscriptions");
        Iterator iter = rootBrokers.entrySet().iterator();
        while (iter.hasNext()) {
            TopicBroker broker = (TopicBroker) ((Map.Entry) iter.next()).getValue();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "removeRemoteSubscriptions: " + routerName + ", removing subscriptions from broker: " + broker);
            broker.removeRemoteSubscriptions(routerName);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "removeRemoteSubscriptions: " + routerName + ", done");
    }

    public synchronized void processTopicInfo(TopicInfo topicInfo) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "processTopicInfo: " + topicInfo);
        String brokerQueue = TOPIC_PREFIX + topicInfo.getTokenizedPredicate()[0];
        TopicBroker broker = (TopicBroker) rootBrokers.get(brokerQueue);
        if (broker == null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), ": creating topic: " + topicInfo.getTopicName());
            createTopic(topicInfo.getTopicName());
            broker = (TopicBroker) rootBrokers.get(brokerQueue);
        }
        broker.processTopicInfo(topicInfo);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "processTopicInfo: " + topicInfo + ", done");
    }

    public void addStaticSubscription(String routerName, String topicName, boolean keepOnUnsubscribe) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "addStaticSubscription, routerName: " + routerName + ", topicName: " + topicName + ", keep: " + keepOnUnsubscribe);
        String brokerQueue = TOPIC_PREFIX + topicName;
        TopicBroker broker = (TopicBroker) rootBrokers.get(brokerQueue);
        if (broker == null) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), ": creating topic: " + topicName);
            createTopic(topicName);
            broker = (TopicBroker) rootBrokers.get(brokerQueue);
        }
        broker.addStaticSubscription(routerName, keepOnUnsubscribe);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "addStaticSubscription, routerName: " + routerName + ", topicName: " + topicName + ", done");
    }

    synchronized void addSlowSubscriberCondition(SlowSubscriberCondition condition) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "addSlowSubscriberCondition, condition: " + condition + " ...");
        String brokerQueue = TOPIC_PREFIX + condition.getTopicName();
        TopicBroker broker = (TopicBroker) rootBrokers.get(brokerQueue);
        if (broker == null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), ": creating topic: " + condition.getTopicName());
            createTopic(condition.getTopicName());
            broker = (TopicBroker) rootBrokers.get(brokerQueue);
        }
        broker.setSlowSubscriberCondition(condition);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "addSlowSubscriberCondition, condition: " + condition + ", done");
    }

    synchronized void removeSlowSubscriberCondition(String topicName) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "removeSlowSubscriberCondition, topicName: " + topicName + " ...");
        String brokerQueue = TOPIC_PREFIX + topicName;
        TopicBroker broker = (TopicBroker) rootBrokers.get(brokerQueue);
        if (broker != null)
            broker.setSlowSubscriberCondition(null);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "removeSlowSubscriberCondition, topicName: " + topicName + ", done");
    }

    synchronized void removeStaticSubscription(String routerName, String topicName) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "removeStaticSubscription, routerName: " + routerName + ", topicName: " + topicName);
        String brokerQueue = TOPIC_PREFIX + topicName;
        TopicBroker broker = (TopicBroker) rootBrokers.get(brokerQueue);
        if (broker != null) {
            broker.removeStaticSubscription(routerName);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "removeStaticSubscription, routerName: " + routerName + ", topicName: " + topicName + ", done");
    }

    private HashMap loadDurables() throws Exception {
        HashMap map = new HashMap();
        durableStore = ctx.storeSwiftlet.getDurableSubscriberStore();
        for (Iterator iter = durableStore.iterator(); iter.hasNext(); ) {
            DurableStoreEntry entry = (DurableStoreEntry) iter.next();
            map.put(entry.getClientId() + DURABLE_DELIMITER + entry.getDurableName(), new DurableSubscription(entry));
        }
        return map;
    }

    private void createStaticTopicSub(Entity routerEntity) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "createStaticTopicSub, router: " + routerEntity.getName() + " ...");
        EntityList stsList = (EntityList) routerEntity.getEntity("static-topic-subscriptions");
        Map entities = stsList.getEntities();
        if (entities != null && entities.size() > 0) {
            for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
                Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
                String[] tt = tokenizeTopicName(entity.getName());
                if (tt.length != 1)
                    throw new Exception(entity.getName() + ": Invalid topic name. Please specify the root node of the topic hierarchie only!");
                SwiftUtilities.verifyClientId(entity.getName());
                addStaticSubscription(routerEntity.getName(), entity.getName(), ((Boolean) entity.getProperty("keep-on-unsubscribe").getValue()).booleanValue());
            }
        }
        stsList.setEntityAddListener(new EntityChangeAdapter(routerEntity.getName()) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                try {
                    String[] tt = tokenizeTopicName(newEntity.getName());
                    if (tt.length != 1)
                        throw new Exception(newEntity.getName() + ": Invalid topic name. Please specify only the root node of the topic hierarchy!");
                    SwiftUtilities.verifyClientId(newEntity.getName());
                    addStaticSubscription((String) configObject, newEntity.getName(), ((Boolean) newEntity.getProperty("keep-on-unsubscribe").getValue()).booleanValue());
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "onEntityAdd (static-topic-subscriptions): new topic=" + newEntity.getName());
                } catch (Exception e) {
                    throw new EntityAddException(e.getMessage());
                }
            }
        });
        stsList.setEntityRemoveListener(new EntityChangeAdapter(routerEntity.getName()) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                try {
                    removeStaticSubscription((String) configObject, delEntity.getName());
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "onEntityRemove (static-topic-subscriptions): del topic=" + delEntity.getName());
                } catch (Exception e) {
                    throw new EntityRemoveException(e.getMessage());
                }
            }
        });
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "createStaticTopicSub, router: " + routerEntity.getName() + " done.");
    }

    private void createStaticRemoteRouterSubs() throws SwiftletException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createStaticRemoteRouterSubs...");
        EntityList srrList = (EntityList) root.getEntity("static-remote-router-subscriptions");
        if (srrList == null)
            return;
        Map entities = srrList.getEntities();
        if (entities != null && entities.size() > 0) {
            for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
                Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
                Route route = ctx.routingSwiftlet.getRoute(entity.getName());
                if (route == null)
                    throw new SwiftletException("Unable to create static remote router subscriptions for router '" + entity.getName() +
                            "', missing route. Please create a static route to '" + entity.getName() + "'");
                try {
                    createStaticTopicSub(entity);
                } catch (Exception e) {
                    throw new SwiftletException(e.getMessage());
                }
            }
        }
        srrList.setEntityAddListener(new EntityChangeAdapter(null) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                try {
                    Route route = ctx.routingSwiftlet.getRoute(newEntity.getName());
                    if (route == null)
                        throw new SwiftletException("Unable to create static remote router subscriptions for router '" + newEntity.getName() +
                                "', missing route. Please create a static route to '" + newEntity.getName() + "'");
                    createStaticTopicSub(newEntity);
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "onEntityAdd (static-remote-router-subscriptions): new router=" + newEntity.getName());
                } catch (Exception e) {
                    throw new EntityAddException(e.getMessage());
                }
            }
        });
        srrList.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                try {
                    EntityList stsList = (EntityList) delEntity.getEntity("static-topic-subscriptions");
                    Map entities = stsList.getEntities();
                    if (entities != null && entities.size() > 0) {
                        for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
                            Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
                            removeStaticSubscription(delEntity.getName(), entity.getName());
                        }
                    }
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "onEntityRemove (static-remote-router-subscriptions): del router=" + delEntity.getName());
                } catch (Exception e) {
                    throw new EntityRemoveException(e.getMessage());
                }
            }
        });
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createStaticRemoteRouterSubs done.");
    }

    private void createSlowSubscriberConditions() throws SwiftletException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createSlowSubscriberConditions...");
        EntityList sspList = (EntityList) root.getEntity("slow-subscriber-conditions");
        Map entities = sspList.getEntities();
        if (entities != null && entities.size() > 0) {
            for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
                Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
                String[] tt = tokenizeTopicName(entity.getName());
                if (tt.length != 1)
                    throw new SwiftletException(entity.getName() + ": Invalid topic name. Please specify only the root node of the topic hierarchy!");
                try {
                    SwiftUtilities.verifyClientId(entity.getName());
                    addSlowSubscriberCondition(new SlowSubscriberCondition(entity));
                } catch (Exception e) {
                    throw new SwiftletException("Error creating slow subscriber condition: " + e.getMessage());
                }
            }
        }
        sspList.setEntityAddListener(new EntityChangeAdapter(null) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                try {
                    String[] tt = tokenizeTopicName(newEntity.getName());
                    if (tt.length != 1)
                        throw new SwiftletException(newEntity.getName() + ": Invalid topic name. Please specify only the root node of the topic hierarchy!");
                    SwiftUtilities.verifyClientId(newEntity.getName());
                    addSlowSubscriberCondition(new SlowSubscriberCondition(newEntity));
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "onEntityAdd (slow-subscriber-conditions): new condition=" + newEntity.getName());
                } catch (Exception e) {
                    throw new EntityAddException(e.getMessage());
                }
            }
        });
        sspList.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                try {
                    removeSlowSubscriberCondition(delEntity.getName());
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "onEntityRemove (slow-subscriber-conditions): del conditions=" + delEntity.getName());
                } catch (Exception e) {
                    throw new EntityRemoveException(e.getMessage());
                }
            }
        });
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createSlowSubscriberPolicies done.");
    }

    protected void startup(Configuration config)
            throws SwiftletException {
        this.config = config;
        root = config;

        ctx = new TopicManagerContext();
        ctx.topicManager = this;
        ctx.traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        ctx.traceSpace = ctx.traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
        ctx.logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
        ctx.storeSwiftlet = (StoreSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$store");
        ctx.queueManager = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
        ctx.authSwiftlet = (AuthenticationSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$authentication");
        ctx.threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
        ctx.timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup ...");

        ctx.activeDurableList = (EntityList) root.getEntity("usage").getEntity("durables");
        ctx.activeSubscriberList = (EntityList) root.getEntity("usage").getEntity("subscriber");
        ctx.remoteSubscriberList = (EntityList) root.getEntity("usage").getEntity("subscriber-remote");
        if (ctx.remoteSubscriberList != null) {
            ctx.remoteSubscriberList.setEntityRemoveListener(new EntityRemoveListener() {
                public void onEntityRemove(Entity parent, Entity delEntity) throws EntityRemoveException {
                    removeRemoteSubscriptions(delEntity.getName());
                }
            });
            new TopicAnnounceSender(ctx);
        }

        SwiftletManager.getInstance().addSwiftletManagerListener("sys$routing", new SwiftletManagerAdapter() {
            public void swiftletStarted(SwiftletManagerEvent evt) {
                try {
                    ctx.routingSwiftlet = (RoutingSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$routing");
                    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "starting topic announcer ...");
                    Route[] routes = ctx.routingSwiftlet.getRoutes();
                    if (routes != null) {
                        for (int i = 0; i < routes.length; i++) {
                            ctx.announceSender.destinationAdded(routes[i]);
                        }
                    }
                    ctx.routingSwiftlet.addRoutingListener(ctx.announceSender);
                } catch (Exception e) {
                    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "swiftletStartet, exception=" + e);
                }
                ctx.announceSender.start();
                if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "creating static remote subscriptions ...");
                try {
                    createStaticRemoteRouterSubs();
                } catch (SwiftletException e) {
                    e.printStackTrace();
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "creating static remote subscriptions ...");
                    ctx.logSwiftlet.logError("sys$topicmanager", e.getMessage());
                }
            }
        });

        SwiftletManager.getInstance().addSwiftletManagerListener("sys$jndi", new SwiftletManagerAdapter() {
            public void swiftletStarted(SwiftletManagerEvent evt) {
                try {
                    ctx.jndiSwiftlet = (JNDISwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$jndi");
                    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "registering JNDI topics ...");
                    Iterator iter = rootBrokers.entrySet().iterator();
                    while (iter.hasNext()) {
                        TopicBroker broker = (TopicBroker) ((Map.Entry) iter.next()).getValue();
                        String[] names = broker.getTopicNames();
                        for (int i = 0; i < names.length; i++) {
                            registerJNDI(names[i], new TopicImpl(names[i]));
                        }
                    }
                } catch (Exception e) {
                    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "swiftletStartet, exception=" + e);
                }
            }
        });
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup: creating topics ...");
        ctx.logSwiftlet.logInformation(getName(), "startup: creating topics ...");
        try {
            createTopics((EntityList) root.getEntity("topics"));
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }

        if (ctx.remoteSubscriberList != null) {
            topicQueue = TOPIC_QUEUE + '@' + SwiftletManager.getInstance().getRouterName();
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "starting TopicAnnounceReceiver ...");
            try {
                new TopicAnnounceReceiver(ctx, topicQueue);
            } catch (Exception e) {
                throw new SwiftletException(e.getMessage());
            }
        }

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "creating durable subscribers ... ");
        try {
            durableSubscriptions = loadDurables();
        } catch (Exception e) {
            throw new SwiftletException("error loading durable subscriptions: " + e);
        }
        Iterator iter = durableSubscriptions.keySet().iterator();
        while (iter.hasNext()) {
            String queueName = (String) iter.next();
            DurableSubscription durable = (DurableSubscription) durableSubscriptions.get(queueName);
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "creating durable subscriber: " + durable);
            durableSubscriptions.put(durable.getQueueName(), durable);
            try {
                ctx.queueManager.createQueue(durable.getQueueName(), (ActiveLogin) null);
                TopicImpl topic = new TopicImpl(getQueueForTopic(durable.getTopicName()), durable.getTopicName());
                // Attempt to create the topic. Must be done for dursubs, because
                // it could be the topic isn't defined but it MUST for dursubs!
                try {
                    createTopic(topic.getTopicName());
                } catch (Exception ignored) {
                }
                ActiveLogin dlogin = ctx.authSwiftlet.createActiveLogin(durable.getClientId(), DURABLE_TYPE);
                dlogin.setClientId(durable.getClientId());
                int id = subscribe(topic, durable.getSelector(),
                        durable.isNoLocal(), durable.getQueueName(), dlogin);
                durable.setTopicSubscription((TopicSubscription) topicSubscriptions.get(id));

                Entity durEntity = ctx.activeDurableList.createEntity();
                durEntity.setName(durable.getQueueName());
                durEntity.setDynamicObject(durable);
                durEntity.createCommands();
                Property prop = durEntity.getProperty("clientid");
                prop.setValue(dlogin.getClientId());
                prop.setReadOnly(true);
                prop = durEntity.getProperty("durablename");
                prop.setValue(durable.getDurableName());
                prop.setReadOnly(true);
                prop = durEntity.getProperty("topic");
                prop.setValue(durable.getTopicName());
                prop.setReadOnly(true);
                prop = durEntity.getProperty("boundto");
                prop.setValue(durable.getQueueName());
                prop.setReadOnly(true);
                prop = durEntity.getProperty("nolocal");
                prop.setValue(new Boolean(durable.isNoLocal()));
                prop.setReadOnly(true);
                prop = durEntity.getProperty("selector");
                if (durable.getSelector() != null) {
                    prop.setValue(durable.getSelector().getConditionString());
                }
                prop.setReadOnly(true);
                ctx.activeDurableList.addEntity(durEntity);
            } catch (Exception e) {
                e.printStackTrace();
                throw new SwiftletException(e.getMessage());
            }
        }
        ctx.activeDurableList.setEntityAddListener(new EntityChangeAdapter(null) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                try {
                    if (programmaticDurableInProgress)
                        return; // do nothing
                    String clientId = (String) newEntity.getProperty("clientid").getValue();
                    SwiftUtilities.verifyClientId(clientId);
                    String durableName = (String) newEntity.getProperty("durablename").getValue();
                    SwiftUtilities.verifyDurableName(durableName);
                    if (!newEntity.getName().equals(clientId + "$" + durableName))
                        throw new Exception("The name of this entity must be: " + clientId + "$" + durableName + " (but it is " + newEntity.getName() + ")");
                    if (clientId.indexOf('@') == -1) {
                        clientId = clientId + "@" + SwiftletManager.getInstance().getRouterName();
                        newEntity.getProperty("clientid").setValue(clientId);
                    }
                    String sel = (String) newEntity.getProperty("selector").getValue();
                    MessageSelector selector = null;
                    if (sel != null) {
                        selector = new MessageSelector(sel);
                        selector.compile();
                    }
                    String topicName = (String) newEntity.getProperty("topic").getValue();
                    if (!isTopicDefined(topicName))
                        throw new Exception("Unknown topic: " + topicName);
                    TopicImpl topic = verifyTopic(new TopicImpl(topicName));
                    boolean noLocal = ((Boolean) newEntity.getProperty("nolocal").getValue()).booleanValue();
                    ActiveLogin dlogin = ctx.authSwiftlet.createActiveLogin(clientId, DURABLE_TYPE);
                    dlogin.setClientId(clientId);
                    subscribeDurable(durableName, topic, selector, noLocal, dlogin, newEntity, false);
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "onEntityAdd (durable): new durable=" + newEntity.getName());
                } catch (Exception e) {
                    throw new EntityAddException(e.getMessage());
                }
            }
        });
        ctx.activeDurableList.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                try {
                    DurableSubscription myDurable = (DurableSubscription) delEntity.getDynamicObject();
                    ActiveLogin myLogin = ctx.authSwiftlet.createActiveLogin(myDurable.getClientId(), "DURABLE");
                    myLogin.setClientId(myDurable.getClientId());
                    deleteDurable(myDurable.getDurableName(), myLogin);
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "onEntityRemove (durable): del durable=" + myDurable);
                } catch (Exception e) {
                    throw new EntityRemoveException(e.getMessage());
                }
            }
        });

        createSlowSubscriberConditions();
        SwiftletManager.getInstance().addSwiftletManagerListener("sys$scheduler", new SwiftletManagerAdapter() {
            public void swiftletStarted(SwiftletManagerEvent event) {
                jobRegistrar = new JobRegistrar(TopicManagerImpl.this, (SchedulerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$scheduler"), ctx);
                jobRegistrar.register();
            }

            public void swiftletStopInitiated(SwiftletManagerEvent event) {
                jobRegistrar.unregister();
            }
        });

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup done.");
    }

    protected void shutdown()
            throws SwiftletException {
        // true if shutdown while standby
        if (ctx == null)
            return;

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown ...");

        if (ctx.announceSender != null) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown: stopping topic announcer ...");
            ctx.announceSender.close();
        }

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown: removing all subcriptions ...");
        for (int i = 0; i < topicSubscriptions.size(); i++) {
            if (topicSubscriptions.get(i) != null)
                unsubscribe(i);
        }
        topicSubscriptions.clear();
        if (durableSubscriptions != null)
            durableSubscriptions.clear();
        if (ctx.announceReceiver != null) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "stopping TopicAnnounceReceiver ...");
            try {
                ctx.announceReceiver.setClosed();
            } catch (Exception ignored) {
            }
            try {
                ctx.queueManager.deleteQueue(topicQueue, false);
            } catch (Exception ignored) {
            }
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "removing all topic brokers ...");
        for (Iterator iter = rootBrokers.entrySet().iterator(); iter.hasNext(); ) {
            TopicBroker b = (TopicBroker) ((Map.Entry) iter.next()).getValue();
            try {
                ctx.queueManager.deleteQueue(TOPIC_PREFIX + b.getRootTopic(), false);
            } catch (Exception ignored) {
            }
        }
        rootBrokers.clear();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown: done.");
        ctx = null;
    }

}

