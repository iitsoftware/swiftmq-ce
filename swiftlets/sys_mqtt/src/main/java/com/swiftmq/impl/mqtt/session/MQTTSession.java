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

package com.swiftmq.impl.mqtt.session;

import com.swiftmq.impl.mqtt.SwiftletContext;
import com.swiftmq.impl.mqtt.connection.MQTTConnection;
import com.swiftmq.impl.mqtt.po.*;
import com.swiftmq.impl.mqtt.pubsub.Producer;
import com.swiftmq.impl.mqtt.pubsub.Subscription;
import com.swiftmq.impl.mqtt.pubsub.SubscriptionStoreEntry;
import com.swiftmq.impl.mqtt.v311.netty.buffer.ByteBuf;
import com.swiftmq.impl.mqtt.v311.netty.handler.codec.mqtt.*;
import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityAddException;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.QueueTransactionClosedException;
import com.swiftmq.tools.collection.ConcurrentList;
import com.swiftmq.util.SwiftUtilities;
import org.magicwerk.brownies.collections.GapList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class MQTTSession extends MQTTVisitorAdapter {
    SwiftletContext ctx;
    String clientId;
    boolean persistent;
    final AtomicReference<MQTTConnection> mqttConnection = new AtomicReference<>();
    final AtomicLong lastUse = new AtomicLong();
    final AtomicBoolean wasPresent = new AtomicBoolean(false);
    final AtomicInteger pid = new AtomicInteger(1);
    final AtomicInteger durableId = new AtomicInteger(0);
    Map<Integer, Producer> producers = new ConcurrentHashMap<>();
    Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();
    Map<Integer, POSendMessage> outboundPackets = new ConcurrentHashMap<>();
    List<ReplayEntry> replayLog = new ConcurrentList<>(new GapList<>());
    final AtomicReference<Entity> registryUsage = new AtomicReference<>();
    final AtomicReference<Entity> connectionUsage = new AtomicReference<>();
    final AtomicInteger msgsReceived = new AtomicInteger();
    final AtomicInteger msgsSent = new AtomicInteger();
    final AtomicInteger totalMsgsReceived = new AtomicInteger();
    final AtomicInteger totalMsgsSent = new AtomicInteger();

    public MQTTSession(SwiftletContext ctx, String clientId, boolean persistent) {
        this.ctx = ctx;
        this.clientId = clientId;
        this.persistent = persistent;
        this.lastUse.set(System.currentTimeMillis());
        try {
            if (persistent)
                ctx.sessionStore.add(clientId, this);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", created");
    }

    public MQTTSession(SwiftletContext ctx, String clientId, SessionStoreEntry sessionStoreEntry) throws Exception {
        this.ctx = ctx;
        this.clientId = clientId;
        this.persistent = true;
        this.pid.set(sessionStoreEntry.pid);
        this.durableId.set(sessionStoreEntry.durableId);
        this.lastUse.set(System.currentTimeMillis());
        for (int i = 0; i < sessionStoreEntry.subscriptionStoreEntries.size(); i++) {
            SubscriptionStoreEntry subscriptionStoreEntry = sessionStoreEntry.subscriptionStoreEntries.get(i);
            subscriptions.put(subscriptionStoreEntry.topicName, new Subscription(ctx, this, subscriptionStoreEntry));
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", created from: " + sessionStoreEntry);
    }

    public boolean isPersistent() {
        return persistent;
    }

    public String getClientId() {
        return clientId;
    }

    public long getLastUse() {
        return lastUse.get();
    }

    public boolean isWasPresent() {
        return wasPresent.get();
    }

    public void setWasPresent(boolean wasPresent) {
        this.wasPresent.set(wasPresent);
    }

    public void associate(MQTTConnection mqttConnection) {
        this.mqttConnection.set(mqttConnection);
        lastUse.set(System.currentTimeMillis());
        try {
            if (registryUsage.get() != null) {
                registryUsage.get().getProperty("associated").setValue(mqttConnection != null);
            }
        } catch (Exception e) {
        }
    }

    public boolean isAssociated() {
        return mqttConnection.get() != null;
    }

    public MQTTConnection getMqttConnection() {
        return mqttConnection.get();
    }

    public int getMsgsReceived() {
        return msgsReceived.getAndSet(0);
    }

    public int getMsgsSent() {
        return msgsSent.getAndSet(0);
    }

    public int getTotalMsgsReceived() {
        return totalMsgsReceived.get();
    }

    public int getTotalMsgsSent() {
        return totalMsgsSent.get();
    }

    private void incMsgsSent(int n) {
        if (msgsSent.get() == Integer.MAX_VALUE)
            msgsSent.set(0);
        msgsSent.addAndGet(n);
        if (totalMsgsSent.get() == Integer.MAX_VALUE)
            totalMsgsSent.set(0);
        totalMsgsSent.addAndGet(n);
    }

    private void incMsgsReceived(int n) {
        if (msgsReceived.get() == Integer.MAX_VALUE)
            msgsReceived.set(0);
        msgsReceived.addAndGet(n);
        if (totalMsgsReceived.get() == Integer.MAX_VALUE)
            totalMsgsReceived.set(0);
        totalMsgsReceived.addAndGet(n);
    }

    private void createRegistryUsage(Subscription subscription) {
        try {
            Entity subUsage = ((EntityList) registryUsage.get().getEntity("subscriptions")).createEntity();
            subUsage.setName(subscription.getTopicName());
            subscription.fillRegistryUsage(subUsage);
            subUsage.createCommands();
            registryUsage.get().getEntity("subscriptions").addEntity(subUsage);
        } catch (EntityAddException e) {
        }
    }

    private void createConnectionUsage(Subscription subscription) {
        try {
            Entity subUsage = ((EntityList) connectionUsage.get().getEntity("subscriptions")).createEntity();
            subUsage.setName(subscription.getTopicName());
            subscription.fillConnectionUsage(subUsage);
            subUsage.createCommands();
            connectionUsage.get().getEntity("subscriptions").addEntity(subUsage);
        } catch (EntityAddException e) {
        }
    }

    public void fillRegistryUsage(Entity registryUsage) {
        this.registryUsage.set(registryUsage);
        try {
            registryUsage.getProperty("associated").setValue(mqttConnection.get() != null);
            registryUsage.getProperty("persistent").setValue(persistent);
        } catch (Exception e) {
        }
        for (Iterator<Map.Entry<String, Subscription>> iter = subscriptions.entrySet().iterator(); iter.hasNext(); ) {
            Subscription subscription = iter.next().getValue();
            createRegistryUsage(subscription);
        }
    }

    public void fillConnectionUsage(Entity connectionUsage) {
        this.connectionUsage.set(connectionUsage);
        try {
            connectionUsage.getProperty("persistent").setValue(persistent);
        } catch (Exception e) {
        }
        for (Iterator<Map.Entry<String, Subscription>> iter = subscriptions.entrySet().iterator(); iter.hasNext(); ) {
            Subscription subscription = iter.next().getValue();
            createConnectionUsage(subscription);
        }
    }

    public SessionStoreEntry getSessionStoreEntry() {
        List<SubscriptionStoreEntry> subscriptionStoreEntries = new ArrayList<>();
        for (Map.Entry<String, Subscription> stringSubscriptionEntry : subscriptions.entrySet()) {
            subscriptionStoreEntries.add(stringSubscriptionEntry.getValue().getStoreEntry());
        }
        return new SessionStoreEntry(durableId.get(), pid.get(), subscriptionStoreEntries);
    }

    public int nextDurableId() {
        if (durableId.get() == Integer.MAX_VALUE)
            durableId.set(0);
        return durableId.getAndIncrement();
    }

    private void addReplay(int packetId, MqttMessage message) {
        replayLog.add(new ReplayEntry(packetId, message));
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", addReplay, size=" + replayLog.size() + ", packetId= " + packetId + ", message=" + message);
    }

    private void removeReplay(int packetId) {
        for (Iterator<ReplayEntry> iter = replayLog.iterator(); iter.hasNext(); ) {
            if (iter.next().packetid == packetId) {
                iter.remove();
                break;
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", removeReplay, size=" + replayLog.size() + " packetId=" + packetId);
    }

    private void replay() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", replay, size= " + replayLog.size());
        for (ReplayEntry replayEntry : replayLog) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", replay, packetId= " + replayEntry.packetid + ", message=" + replayEntry.message);
            mqttConnection.get().getOutboundQueue().submit(replayEntry.message);
        }
    }

    public String topicNameTranslate(String mqttTopicName) throws Exception {
        if (mqttTopicName.indexOf('#') != -1 || mqttTopicName.indexOf('+') != -1)
            throw new Exception("Wildcards are not allowed in topic names");
        return mqttTopicName.replaceAll("/", ".");
    }

    private String topicNameTranslateReverse(String mqttTopicName) {
        return mqttTopicName.replaceAll("\\.", "/");
    }

    private String topicFilterTranslate(String mqttTopicFilter) throws Exception {
        String[] tokenized = SwiftUtilities.tokenize(mqttTopicFilter, "/");
        if (tokenized.length == 0 || tokenized[0].startsWith("$"))
            throw new Exception("SwiftMQ does not support '$' (system topics): " + mqttTopicFilter);
        if (tokenized[0].equals("#") || tokenized[0].equals("+"))
            throw new Exception("SwiftMQ does not support wildcards at the root node of a topic filter: " + mqttTopicFilter);
        for (int i = 0; i < tokenized.length; i++) {
            if (tokenized[i].contains("#") && (i != tokenized.length - 1 || tokenized[i].length() > 1))
                throw new Exception("Multilevel wildcard '#' must stand on its own and must be the last character of a topic filter: " + mqttTopicFilter);
            if (tokenized[i].contains("+") && tokenized[i].length() > 1)
                throw new Exception("Singlelevel wildcard '+' must stand on its own in a topic filter: " + mqttTopicFilter);
            if (tokenized[i].contains("%"))
                throw new Exception("SwiftMQ wildcard '%' is not allowed in a MQTT topic filter: " + mqttTopicFilter);
            if (tokenized[i].isEmpty())
                throw new Exception("Invalid topic filter: " + mqttTopicFilter);
        }
        return SwiftUtilities.concat(tokenized, ".").replaceAll("\\+", "%").replaceAll("#", "%");
    }

    private int subscribe(MqttTopicSubscription subscription) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", subscribe: " + subscription.topicName());
        try {
            Subscription sub = subscriptions.remove(subscription.topicName());
            if (sub != null) {
                sub.stop();
                sub.close();
                if (registryUsage.get() != null) {
                    registryUsage.get().getEntity("subscriptions").removeEntity(registryUsage.get().getEntity("subscriptions").getEntity(sub.getTopicName()));
                }
                if (connectionUsage.get() != null) {
                    connectionUsage.get().getEntity("subscriptions").removeEntity(connectionUsage.get().getEntity("subscriptions").getEntity(sub.getTopicName()));
                }
            }
            String topicNameTranslated = topicFilterTranslate(subscription.topicName());
            sub = new Subscription(ctx, this, subscription.topicName(), topicNameTranslated, subscription.qualityOfService());
            subscriptions.put(subscription.topicName(), sub);
            sub.start();
            if (persistent) {
                ctx.sessionStore.remove(clientId);
                ctx.sessionStore.add(clientId, this);
                createRegistryUsage(sub);
            }
            if (connectionUsage.get() != null)
                createConnectionUsage(sub);
            return subscription.qualityOfService().value();
        } catch (Exception e) {
            ctx.logSwiftlet.logError(ctx.mqttSwiftlet.getName(), this + "/Subscribe exception: " + e.getMessage());
            return MqttQoS.FAILURE.value();
        }
    }

    private void unsubscribe(String topicFilter) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", unsubscribe: " + topicFilter);
        Subscription subscription = subscriptions.remove(topicFilter);
        if (subscription != null) {
            subscription.stop();
            subscription.close();
            if (registryUsage.get() != null) {
                registryUsage.get().getEntity("subscriptions").removeEntity(registryUsage.get().getEntity("subscriptions").getEntity(subscription.getTopicName()));
            }
            if (connectionUsage.get() != null) {
                connectionUsage.get().getEntity("subscriptions").removeEntity(connectionUsage.get().getEntity("subscriptions").getEntity(subscription.getTopicName()));
            }
        }
        if (persistent) {
            ctx.sessionStore.remove(clientId);
            ctx.sessionStore.add(clientId, this);
        }
    }

    public void unsubscribe(String topicFilter, ActiveLogin activeLogin) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", unsubscribe: " + topicFilter);
        Subscription subscription = subscriptions.remove(topicFilter);
        if (subscription != null) {
            subscription.close(activeLogin);
        }
        if (persistent) {
            ctx.sessionStore.remove(clientId);
            ctx.sessionStore.add(clientId, this);
        }
    }

    public void start() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", start ...");
        replay();
        for (Map.Entry<String, Subscription> stringSubscriptionEntry : subscriptions.entrySet()) {
            Subscription subscription = stringSubscriptionEntry.getValue();
            try {
                subscription.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", start done");
    }

    public void stop() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", stop ...");
        for (Map.Entry<String, Subscription> stringSubscriptionEntry : subscriptions.entrySet()) {
            Subscription subscription = stringSubscriptionEntry.getValue();
            subscription.stop();
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", stop done");
    }

    public void destroy() {
        destroy(true);
    }

    public void destroy(ActiveLogin activeLogin, boolean removeUsage) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", destroy ...");
        for (Map.Entry<String, Subscription> stringSubscriptionEntry : subscriptions.entrySet()) {
            Subscription subscription = stringSubscriptionEntry.getValue();
            subscription.close(activeLogin);
        }
        subscriptions.clear();
        try {
            if (removeUsage && persistent)
                ctx.sessionStore.remove(clientId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", destroy done");
    }

    public void destroy(boolean removeUsage) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", destroy ...");
        for (Map.Entry<String, Subscription> stringSubscriptionEntry : subscriptions.entrySet()) {
            Subscription subscription = stringSubscriptionEntry.getValue();
            subscription.close();
        }
        subscriptions.clear();
        try {
            if (removeUsage && persistent)
                ctx.sessionStore.remove(clientId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", destroy done");
    }

    @Override
    public void visit(POPublish po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " ...");
        MqttPublishMessage publishMessage = po.getMessage();
        MqttFixedHeader fixedHeader = publishMessage.fixedHeader();
        MqttPublishVariableHeader variableHeader = publishMessage.variableHeader();
        try {
            String topicName = topicNameTranslate(variableHeader.topicName());
            if (fixedHeader.isRetain())
                ctx.retainer.add(topicName, publishMessage);
            Producer producer = new Producer(ctx, this, topicName);
            producer.send(publishMessage);
            int packetId = variableHeader.packetId();
            MqttQoS qos = fixedHeader.qosLevel();
            switch (qos) {
                case AT_MOST_ONCE:
                    producer.commit();
                    break;
                case AT_LEAST_ONCE:
                    producer.commit();
                    mqttConnection.get().getOutboundQueue().submit(
                            new MqttPubAckMessage(new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_LEAST_ONCE, false, 2),
                                    MqttMessageIdVariableHeader.from(packetId))
                    );
                    break;
                case EXACTLY_ONCE:
                    producers.put(packetId, producer);
                    mqttConnection.get().getOutboundQueue().submit(
                            new MqttMessage(new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_LEAST_ONCE, false, 2),
                                    MqttMessageIdVariableHeader.from(packetId))
                    );
                    break;
                default:
                    mqttConnection.get().initiateClose("publish: invalid qos=" + qos.value());
            }
            incMsgsSent(1);
        } catch (Exception e) {
            mqttConnection.get().initiateClose("publish: exception=" + e);
        }

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POSendMessage po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " ...");
        try {
            BytesMessageImpl jmsMessage = (BytesMessageImpl) po.getJmsMessage();
            jmsMessage.reset();
            MqttQoS qos = po.getQos();
            String topicName = topicNameTranslateReverse(po.getTopicName());
            byte[] b = new byte[(int) jmsMessage.getBodyLength()];
            jmsMessage.readBytes(b);
            ByteBuf byteBuf = new ByteBuf(b);
            byteBuf.reset();
            int packetId = -1;
            if (qos != MqttQoS.AT_MOST_ONCE) {
                if (pid.get() == 65535)
                    pid.set(1);
                packetId = pid.getAndIncrement();
                outboundPackets.put(packetId, po);
            }
            mqttConnection.get().getOutboundQueue().submit(
                    new MqttPublishMessage(new MqttFixedHeader(MqttMessageType.PUBLISH, false, qos, false, 0),
                            new MqttPublishVariableHeader(topicName, packetId), byteBuf)
            );
            if (qos == MqttQoS.AT_MOST_ONCE) {
                po.getTransaction().commit();
                po.getSubscription().restart();
            } else
                addReplay(packetId, new MqttPublishMessage(new MqttFixedHeader(MqttMessageType.PUBLISH, true, qos, false, 0),
                        new MqttPublishVariableHeader(topicName, packetId), byteBuf));
            incMsgsReceived(1);
        } catch (QueueTransactionClosedException qtc) {

        } catch (Exception e) {
            mqttConnection.get().initiateClose("send message: exception=" + e);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POPubAck po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " ...");

        MqttPubAckMessage pubAckMessage = po.getMessage();
        POSendMessage poSendMessage = outboundPackets.remove(pubAckMessage.variableHeader().messageId());
        if (poSendMessage == null || poSendMessage.getQos() != MqttQoS.AT_LEAST_ONCE) {
            mqttConnection.get().getConnectionQueue().enqueue(new POProtocolError(pubAckMessage));
        } else {
            try {
                removeReplay(pubAckMessage.variableHeader().messageId());
                poSendMessage.getTransaction().commit();
                poSendMessage.getSubscription().restart();
            } catch (Exception e) {
                mqttConnection.get().initiateClose("puback: exception=" + e);
            }
        }

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POPubRec po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " ...");
        int packetId = ((MqttMessageIdVariableHeader) (po.getMessage().variableHeader())).messageId();
        POSendMessage poSendMessage = outboundPackets.get(packetId);
        if (poSendMessage == null || poSendMessage.getQos() != MqttQoS.EXACTLY_ONCE) {
            mqttConnection.get().getConnectionQueue().enqueue(new POProtocolError(po.getMessage()));
        } else {
            try {
                mqttConnection.get().getOutboundQueue().submit(
                        new MqttMessage(new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 2),
                                MqttMessageIdVariableHeader.from(packetId))
                );
                removeReplay(packetId); // Remove Publish
                addReplay(packetId, new MqttMessage(new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 2),
                        MqttMessageIdVariableHeader.from(packetId)));
            } catch (Exception e) {
                mqttConnection.get().initiateClose("pubrec: exception=" + e);
            }
        }

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POPubRel po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " ...");
        int packetId = ((MqttMessageIdVariableHeader) (po.getMessage().variableHeader())).messageId();
        Producer producer = producers.remove(packetId);
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + ", packetId=" + packetId + ", producer=" + producer);
            if (producer != null)
                producer.commit();
            mqttConnection.get().getOutboundQueue().submit(
                    new MqttMessage(new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_LEAST_ONCE, false, 2),
                            MqttMessageIdVariableHeader.from(packetId))
            );
        } catch (Exception e) {
            mqttConnection.get().initiateClose("pubrel: exception=" + e);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POPubComp po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " ...");
        int packetId = ((MqttMessageIdVariableHeader) (po.getMessage().variableHeader())).messageId();
        removeReplay(packetId);
        POSendMessage poSendMessage = outboundPackets.remove(packetId);
        if (poSendMessage == null || poSendMessage.getQos() != MqttQoS.EXACTLY_ONCE)
            mqttConnection.get().getConnectionQueue().enqueue(new POProtocolError(po.getMessage()));
        else {
            try {
                if (!poSendMessage.getTransaction().isClosed()) {
                    poSendMessage.getTransaction().commit();
                    poSendMessage.getSubscription().restart();
                }
            } catch (Exception e) {
                mqttConnection.get().initiateClose("pubcomp: exception=" + e);
            }
        }

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POSubscribe po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " ...");

        MqttSubscribeMessage subscribeMessage = po.getMessage();
        MqttMessageIdVariableHeader variableHeader = subscribeMessage.variableHeader();
        MqttSubscribePayload payload = subscribeMessage.payload();

        List<Integer> grantedQoS = new ArrayList<Integer>();
        List<MqttTopicSubscription> subscriptions = payload.topicSubscriptions();
        for (MqttTopicSubscription subscription : subscriptions) {
            grantedQoS.add(subscribe(subscription));
        }
        MqttSubAckPayload subAckPayload = new MqttSubAckPayload(grantedQoS);
        mqttConnection.get().getOutboundQueue().submit(
                new MqttSubAckMessage(new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                        variableHeader, subAckPayload)
        );
        for (MqttTopicSubscription subscription : subscriptions) {
            try {
                List<MqttPublishMessage> retained = ctx.retainer.get(topicFilterTranslate(subscription.topicName()));
                for (MqttPublishMessage publishMessage : retained) {
                    mqttConnection.get().getOutboundQueue().submit(
                            new MqttPublishMessage(new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, true, 0),
                                    new MqttPublishVariableHeader(publishMessage.variableHeader().topicName(), 0), publishMessage.payload())
                    );
                }
            } catch (Exception e) {
                mqttConnection.get().initiateClose("subscribe: exception=" + e);
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POUnsubscribe po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " ...");

        MqttUnsubscribeMessage unsubscribeMessage = po.getMessage();
        MqttMessageIdVariableHeader variableHeader = unsubscribeMessage.variableHeader();
        MqttUnsubscribePayload payload = unsubscribeMessage.payload();

        try {
            List<String> filters = payload.topics();
            for (String filter : filters) {
                unsubscribe(filter);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        mqttConnection.get().getOutboundQueue().submit(
                new MqttMessage(new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_LEAST_ONCE, false, 2),
                        variableHeader, null)
        );

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POCollect po) {
        if (connectionUsage.get() == null)
            return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " ...");
        for (Map.Entry<String, Subscription> stringSubscriptionEntry : subscriptions.entrySet()) {
            Subscription subscription = stringSubscriptionEntry.getValue();
            Property receivedSecProp = connectionUsage.get().getEntity("subscriptions").getEntity(subscription.getTopicName()).getProperty("msgs-received");
            Property receivedTotalProp = connectionUsage.get().getEntity("subscriptions").getEntity(subscription.getTopicName()).getProperty("total-received");
            long received = subscription.getMsgsReceived();
            int totalReceived = subscription.getTotalMsgsReceived();
            double deltasec = Math.max(1.0, (double) (System.currentTimeMillis() - po.getLastCollect()) / 1000.0);
            double ssec = ((double) received / deltasec) + 0.5;
            try {
                if ((Integer) receivedSecProp.getValue() != ssec)
                    receivedSecProp.setValue((int) ssec);
                if ((Integer) receivedTotalProp.getValue() != totalReceived)
                    receivedTotalProp.setValue(totalReceived);
            } catch (Exception e) {
            }
        }

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " done");
    }

    @Override
    public String toString() {
        return (mqttConnection.get() != null ? (mqttConnection + "/") : "") + "MQTTSession, " +
                "clientId='" + clientId + '\'' +
                ", persistent=" + persistent;
    }

    private static class ReplayEntry {
        int packetid;
        MqttMessage message;

        public ReplayEntry(int packetid, MqttMessage message) {
            this.packetid = packetid;
            this.message = message;
        }
    }
}
