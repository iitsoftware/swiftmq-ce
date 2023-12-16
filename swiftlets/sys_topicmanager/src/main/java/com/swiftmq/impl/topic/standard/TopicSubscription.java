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

import com.swiftmq.impl.topic.standard.announce.TopicInfo;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.QueueManager;
import com.swiftmq.swiftlet.queue.Selector;
import com.swiftmq.swiftlet.topic.TopicManager;
import com.swiftmq.tools.collection.ConcurrentList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TopicSubscription {
    final AtomicInteger subscriberId = new AtomicInteger();
    final AtomicInteger brokerSubscriberId = new AtomicInteger();
    TopicBroker broker;
    String topicName;
    String[] tokenizedName;
    Selector selector;
    boolean noLocal;
    ActiveLogin activeLogin;
    String subscriberQueueName;
    boolean remote = false;
    boolean staticSubscription = false;
    boolean keepOnUnsubscribe = false;
    boolean announced = false;
    boolean removePending = false;
    boolean durable = false;
    boolean forceCopy = false;
    String destination = null;
    com.swiftmq.swiftlet.queue.QueueSender queueSender = null;
    TopicManager topicManager = null;
    QueueManager queueManager = null;
    List<TopicSubscriberTransaction> transactions = new ConcurrentList<>(new ArrayList<>());

    protected TopicSubscription(int subscriberId, String topicName, String[] tokenizedName, boolean noLocal, Selector selector, ActiveLogin activeLogin, String subscriberQueueName) {
        this.subscriberId.set(subscriberId);
        this.tokenizedName = tokenizedName;
        this.topicName = topicName;
        this.selector = selector;
        this.noLocal = noLocal;
        this.activeLogin = activeLogin;
        this.subscriberQueueName = subscriberQueueName;
        topicManager = (TopicManager) SwiftletManager.getInstance().getSwiftlet("sys$topicmanager");
        queueManager = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
    }

    protected TopicSubscription(TopicInfo topicInfo, TopicBroker broker) {
        topicManager = (TopicManager) SwiftletManager.getInstance().getSwiftlet("sys$topicmanager");
        queueManager = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
        this.topicName = topicInfo.getTopicName();
        this.tokenizedName = topicInfo.getTokenizedPredicate();
        this.subscriberQueueName = topicManager.getQueueForTopic(this.topicName) + '@' + topicInfo.getRouterName();
        this.broker = broker;
        this.destination = topicInfo.getRouterName();
        remote = true;
    }

    public boolean isForceCopy() {
        return forceCopy;
    }

    public void setForceCopy(boolean forceCopy) {
        this.forceCopy = forceCopy;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public boolean isStaticSubscription() {
        return staticSubscription;
    }

    public void setStaticSubscription(boolean staticSubscription) {
        this.staticSubscription = staticSubscription;
    }

    public void setKeepOnUnsubscribe(boolean keepOnUnsubscribe) {
        this.keepOnUnsubscribe = keepOnUnsubscribe;
    }

    public boolean isKeepOnUnsubscribe() {
        return keepOnUnsubscribe;
    }

    public boolean isAnnounced() {
        return announced;
    }

    public void setAnnounced(boolean announced) {
        this.announced = announced;
    }

    public boolean isRemote() {
        return remote;
    }

    public void setSubscriberId(int subscriberId) {
        this.subscriberId.set(subscriberId);
    }

    public int getSubscriberId() {
        return (subscriberId.get());
    }

    public void setBrokerSubscriberId(int brokerSubscriberId) {
        this.brokerSubscriberId.set(brokerSubscriberId);
    }

    public int getBrokerSubscriberId() {
        return (brokerSubscriberId.get());
    }

    public void setBroker(TopicBroker broker) {
        this.broker = broker;
    }

    public TopicBroker getBroker() {
        return (broker);
    }

    public String getTopicName() {
        return (topicName);
    }

    public String[] getTokenizedName() {
        return (tokenizedName);
    }

    public Selector getSelector() {
        return (selector);
    }

    public boolean isNoLocal() {
        return (noLocal);
    }

    public ActiveLogin getActiveLogin() {
        return (activeLogin);
    }

    public String getDestination() {
        return (destination);
    }

    public String getSubscriberQueueName() {
        return (subscriberQueueName);
    }

    public int getNumberSubscriberQueueMessages() throws Exception {
        if (queueSender == null)
            queueSender = queueManager.createQueueSender(subscriberQueueName, activeLogin);
        return (int) queueSender.getNumberQueueMessages();
    }

    public boolean isRemovePending() {
        return removePending;
    }

    public void setRemovePending(boolean removePending) {
        this.removePending = removePending;
    }

    protected TopicSubscriberTransaction createTransaction()
            throws Exception {
        if (queueSender == null)
            queueSender = queueManager.createQueueSender(subscriberQueueName, activeLogin);
        TopicSubscriberTransaction transaction = new TopicSubscriberTransaction(this, queueSender.createTransaction(), destination);
        transactions.add(transaction);
        return transaction;
    }

    protected void removeTransaction(TopicSubscriberTransaction transaction) {
        transactions.remove(transaction);
    }

    protected void unsubscribe() {
        broker.unsubscribe(this);
        for (TopicSubscriberTransaction topicSubscriberTransaction : transactions) {
            TopicSubscriberTransaction transaction = topicSubscriberTransaction;
            if (transaction != null) {
                try {
                    transaction.rollback();
                } catch (Exception ignored) {
                }
            }
        }
        transactions.clear();
        try {
            queueSender.close();
        } catch (Exception ignored) {
        }
    }

    public boolean equals(Object o) {
        // local subscriptions
        if (activeLogin != null)
            return super.equals(o);

        // remote subscriptions
        TopicSubscription that = (TopicSubscription) o;
        return topicName.equals(that.getTopicName()) &&
                subscriberQueueName.equals(that.getSubscriberQueueName());
    }

    public String toString() {
        StringBuffer s = new StringBuffer();
        s.append("[TopicSubscription, subscriberId=");
        s.append(subscriberId.get());
        s.append(", brokerSubscriberId=");
        s.append(brokerSubscriberId.get());
        s.append(", topicName=");
        s.append(topicName);
        s.append(", selector=");
        s.append(selector);
        s.append(", noLocal=");
        s.append(noLocal);
        s.append(", activeLogin=");
        s.append(activeLogin);
        s.append(", destination=");
        s.append(destination);
        s.append(", subscriberQueueName=");
        s.append(subscriberQueueName);
        s.append(", remote=");
        s.append(remote);
        s.append(", staticSubscription=");
        s.append(staticSubscription);
        s.append(", keepOnUnsubscribe=");
        s.append(keepOnUnsubscribe);
        s.append(", announced=");
        s.append(announced);
        s.append(", removePending=");
        s.append(removePending);
        s.append("]");
        return s.toString();
    }
}

