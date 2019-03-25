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
import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityAddException;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.Property;
import com.swiftmq.mqtt.v311.netty.buffer.ByteBuf;
import com.swiftmq.mqtt.v311.netty.handler.codec.mqtt.*;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.QueueTransactionClosedException;
import com.swiftmq.util.SwiftUtilities;
import org.magicwerk.brownies.collections.GapList;

import java.util.*;

public class MQTTSession extends MQTTVisitorAdapter {
    SwiftletContext ctx;
    String clientId;
    boolean persistent;
    MQTTConnection mqttConnection;
    long lastUse;
    boolean wasPresent = false;
    int pid = 1;
    int durableId = 0;
    Map<Integer, Producer> producers = new HashMap<Integer, Producer>();
    Map<String, Subscription> subscriptions = new HashMap<String, Subscription>();
    Map<Integer, POSendMessage> outboundPackets = new HashMap<Integer, POSendMessage>();
    List<ReplayEntry> replayLog = new GapList<ReplayEntry>();
    Entity registryUsage = null;
    Entity connectionUsage = null;
    volatile int msgsReceived = 0;
    volatile int msgsSent = 0;
    volatile int totalMsgsReceived = 0;
    volatile int totalMsgsSent = 0;

    public MQTTSession(SwiftletContext ctx, String clientId, boolean persistent) {
        this.ctx = ctx;
        this.clientId = clientId;
        this.persistent = persistent;
        this.lastUse = System.currentTimeMillis();
        try {
            if (persistent)
                ctx.sessionStore.add(clientId, this);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", created");
    }

    public MQTTSession(SwiftletContext ctx, String clientId, SessionStoreEntry sessionStoreEntry) throws Exception {
        this.ctx = ctx;
        this.clientId = clientId;
        this.persistent = true;
        this.pid = sessionStoreEntry.pid;
        this.durableId = sessionStoreEntry.durableId;
        this.lastUse = System.currentTimeMillis();
        for (int i = 0; i < sessionStoreEntry.subscriptionStoreEntries.size(); i++) {
            SubscriptionStoreEntry subscriptionStoreEntry = sessionStoreEntry.subscriptionStoreEntries.get(i);
            subscriptions.put(subscriptionStoreEntry.topicName, new Subscription(ctx, this, subscriptionStoreEntry));
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", created from: " + sessionStoreEntry);
    }

    public boolean isPersistent() {
        return persistent;
    }

    public String getClientId() {
        return clientId;
    }

    public long getLastUse() {
        return lastUse;
    }

    public boolean isWasPresent() {
        return wasPresent;
    }

    public void setWasPresent(boolean wasPresent) {
        this.wasPresent = wasPresent;
    }

    public void associate(MQTTConnection mqttConnection) {
        this.mqttConnection = mqttConnection;
        lastUse = System.currentTimeMillis();
        try {
            if (registryUsage != null) {
                registryUsage.getProperty("associated").setValue(new Boolean(mqttConnection != null));
            }
        } catch (Exception e) {
        }
    }

    public boolean isAssociated() {
        return mqttConnection != null;
    }

    public MQTTConnection getMqttConnection() {
        return mqttConnection;
    }

    public int getMsgsReceived() {
        int n = msgsReceived;
        msgsReceived = 0;
        return n;
    }

    public int getMsgsSent() {
        int n = msgsSent;
        msgsSent = 0;
        return n;
    }

    public int getTotalMsgsReceived() {
        return totalMsgsReceived;
    }

    public int getTotalMsgsSent() {
        return totalMsgsSent;
    }

    private void incMsgsSent(int n) {
        if (msgsSent == Integer.MAX_VALUE)
            msgsSent = 0;
        msgsSent += n;
        if (totalMsgsSent == Integer.MAX_VALUE)
            totalMsgsSent = 0;
        totalMsgsSent += n;
    }

    private void incMsgsReceived(int n) {
        if (msgsReceived == Integer.MAX_VALUE)
            msgsReceived = 0;
        msgsReceived += n;
        if (totalMsgsReceived == Integer.MAX_VALUE)
            totalMsgsReceived = 0;
        totalMsgsReceived += n;
    }

    private void createRegistryUsage(Subscription subscription) {
        try {
            Entity subUsage = ((EntityList) registryUsage.getEntity("subscriptions")).createEntity();
            subUsage.setName(subscription.getTopicName());
            subscription.fillRegistryUsage(subUsage);
            subUsage.createCommands();
            registryUsage.getEntity("subscriptions").addEntity(subUsage);
        } catch (EntityAddException e) {
        }
    }

    private void createConnectionUsage(Subscription subscription) {
        try {
            Entity subUsage = ((EntityList) connectionUsage.getEntity("subscriptions")).createEntity();
            subUsage.setName(subscription.getTopicName());
            subscription.fillConnectionUsage(subUsage);
            subUsage.createCommands();
            connectionUsage.getEntity("subscriptions").addEntity(subUsage);
        } catch (EntityAddException e) {
        }
    }

    public void fillRegistryUsage(Entity registryUsage) {
        this.registryUsage = registryUsage;
        try {
            registryUsage.getProperty("associated").setValue(new Boolean(mqttConnection != null));
            registryUsage.getProperty("persistent").setValue(new Boolean(persistent));
        } catch (Exception e) {
        }
        for (Iterator<Map.Entry<String, Subscription>> iter = subscriptions.entrySet().iterator(); iter.hasNext(); ) {
            Subscription subscription = iter.next().getValue();
            createRegistryUsage(subscription);
        }
    }

    public void fillConnectionUsage(Entity connectionUsage) {
        this.connectionUsage = connectionUsage;
        try {
            connectionUsage.getProperty("persistent").setValue(new Boolean(persistent));
        } catch (Exception e) {
        }
        for (Iterator<Map.Entry<String, Subscription>> iter = subscriptions.entrySet().iterator(); iter.hasNext(); ) {
            Subscription subscription = iter.next().getValue();
            createConnectionUsage(subscription);
        }
    }

    public SessionStoreEntry getSessionStoreEntry() {
        List<SubscriptionStoreEntry> subscriptionStoreEntries = new ArrayList<SubscriptionStoreEntry>();
        for (Iterator<Map.Entry<String, Subscription>> iter = subscriptions.entrySet().iterator(); iter.hasNext(); ) {
            subscriptionStoreEntries.add(iter.next().getValue().getStoreEntry());
        }
        return new SessionStoreEntry(durableId, pid, subscriptionStoreEntries);
    }

    public int nextDurableId() {
        if (durableId == Integer.MAX_VALUE)
            durableId = 0;
        int id = durableId;
        durableId++;
        return id;
    }

    private void addReplay(int packetId, MqttMessage message) {
        replayLog.add(new ReplayEntry(packetId, message));
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", addReplay, size=" + replayLog.size() + ", packetId= " + packetId + ", message=" + message);
    }

    private void removeReplay(int packetId) {
        for (Iterator<ReplayEntry> iter = replayLog.listIterator(); iter.hasNext(); ) {
            if (iter.next().packetid == packetId) {
                iter.remove();
                break;
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", removeReplay, size=" + replayLog.size() + " packetId=" + packetId);
    }

    private void replay() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", replay, size= " + replayLog.size());
        for (Iterator<ReplayEntry> iter = replayLog.listIterator(); iter.hasNext(); ) {
            ReplayEntry replayEntry = iter.next();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", replay, packetId= " + replayEntry.packetid + ", message=" + replayEntry.message);
            mqttConnection.getOutboundQueue().enqueue(replayEntry.message);
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
            if (tokenized[i].length() == 0)
                throw new Exception("Invalid topic filter: " + mqttTopicFilter);
        }
        return SwiftUtilities.concat(tokenized, ".").replaceAll("\\+", "%").replaceAll("#", "%");
    }

    private int subscribe(MqttTopicSubscription subscription) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", subscribe: " + subscription.topicName());
        try {
            Subscription sub = subscriptions.remove(subscription.topicName());
            if (sub != null) {
                sub.stop();
                sub.close();
                if (registryUsage != null) {
                    registryUsage.getEntity("subscriptions").removeEntity(registryUsage.getEntity("subscriptions").getEntity(sub.getTopicName()));
                }
                if (connectionUsage != null) {
                    connectionUsage.getEntity("subscriptions").removeEntity(connectionUsage.getEntity("subscriptions").getEntity(sub.getTopicName()));
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
            if (connectionUsage != null)
                createConnectionUsage(sub);
            return subscription.qualityOfService().value();
        } catch (Exception e) {
            ctx.logSwiftlet.logError(ctx.mqttSwiftlet.getName(), toString() + "/Subscribe exception: " + e.getMessage());
            return MqttQoS.FAILURE.value();
        }
    }

    private void unsubscribe(String topicFilter) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", unsubscribe: " + topicFilter);
        Subscription subscription = subscriptions.remove(topicFilter);
        if (subscription != null) {
            subscription.stop();
            subscription.close();
            if (registryUsage != null) {
                registryUsage.getEntity("subscriptions").removeEntity(registryUsage.getEntity("subscriptions").getEntity(subscription.getTopicName()));
            }
            if (connectionUsage != null) {
                connectionUsage.getEntity("subscriptions").removeEntity(connectionUsage.getEntity("subscriptions").getEntity(subscription.getTopicName()));
            }
        }
        if (persistent) {
            ctx.sessionStore.remove(clientId);
            ctx.sessionStore.add(clientId, this);
        }
    }

    public void unsubscribe(String topicFilter, ActiveLogin activeLogin) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", unsubscribe: " + topicFilter);
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
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", start ...");
        replay();
        for (Iterator<Map.Entry<String, Subscription>> iter = subscriptions.entrySet().iterator(); iter.hasNext(); ) {
            Subscription subscription = iter.next().getValue();
            try {
                subscription.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", start done");
    }

    public void stop() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", stop ...");
        for (Iterator<Map.Entry<String, Subscription>> iter = subscriptions.entrySet().iterator(); iter.hasNext(); ) {
            Subscription subscription = iter.next().getValue();
            subscription.stop();
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", stop done");
    }

    public void destroy() {
        destroy(true);
    }

    public void destroy(ActiveLogin activeLogin, boolean removeUsage) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", destroy ...");
        for (Iterator<Map.Entry<String, Subscription>> iter = subscriptions.entrySet().iterator(); iter.hasNext(); ) {
            Subscription subscription = iter.next().getValue();
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
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", destroy done");
    }

    public void destroy(boolean removeUsage) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", destroy ...");
        for (Iterator<Map.Entry<String, Subscription>> iter = subscriptions.entrySet().iterator(); iter.hasNext(); ) {
            Subscription subscription = iter.next().getValue();
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
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", destroy done");
    }

    @Override
    public void visit(POPublish po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
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
                    mqttConnection.getOutboundQueue().enqueue(
                            new MqttPubAckMessage(new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_LEAST_ONCE, false, 2),
                                    MqttMessageIdVariableHeader.from(packetId))
                    );
                    break;
                case EXACTLY_ONCE:
                    producers.put(packetId, producer);
                    mqttConnection.getOutboundQueue().enqueue(
                            new MqttMessage(new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_LEAST_ONCE, false, 2),
                                    MqttMessageIdVariableHeader.from(packetId))
                    );
                    break;
                default:
                    mqttConnection.initiateClose("publish: invalid qos=" + qos.value());
            }
            incMsgsSent(1);
        } catch (Exception e) {
            mqttConnection.initiateClose("publish: exception=" + e);
        }

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POSendMessage po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        try {
            BytesMessageImpl jmsMessage = (BytesMessageImpl) po.getJmsMessage();
            jmsMessage.reset();
            MqttQoS qos = po.getQos();
            String topicName = topicNameTranslateReverse(po.getTopicName());
            byte b[] = new byte[(int) jmsMessage.getBodyLength()];
            jmsMessage.readBytes(b);
            ByteBuf byteBuf = new ByteBuf(b);
            byteBuf.reset();
            int packetId = -1;
            if (qos != MqttQoS.AT_MOST_ONCE) {
                if (pid == 65535)
                    pid = 1;
                packetId = pid++;
                outboundPackets.put(packetId, po);
            }
            mqttConnection.getOutboundQueue().enqueue(
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
            mqttConnection.initiateClose("send message: exception=" + e);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POPubAck po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");

        MqttPubAckMessage pubAckMessage = po.getMessage();
        POSendMessage poSendMessage = outboundPackets.remove(pubAckMessage.variableHeader().messageId());
        if (poSendMessage == null || poSendMessage.getQos() != MqttQoS.AT_LEAST_ONCE) {
            mqttConnection.getConnectionQueue().enqueue(new POProtocolError(pubAckMessage));
        } else {
            try {
                removeReplay(pubAckMessage.variableHeader().messageId());
                poSendMessage.getTransaction().commit();
                poSendMessage.getSubscription().restart();
            } catch (Exception e) {
                mqttConnection.initiateClose("puback: exception=" + e);
            }
        }

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POPubRec po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        int packetId = ((MqttMessageIdVariableHeader) (po.getMessage().variableHeader())).messageId();
        POSendMessage poSendMessage = outboundPackets.get(packetId);
        if (poSendMessage == null || poSendMessage.getQos() != MqttQoS.EXACTLY_ONCE) {
            mqttConnection.getConnectionQueue().enqueue(new POProtocolError(po.getMessage()));
        } else {
            try {
                mqttConnection.getOutboundQueue().enqueue(
                        new MqttMessage(new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 2),
                                MqttMessageIdVariableHeader.from(packetId))
                );
                removeReplay(packetId); // Remove Publish
                addReplay(packetId, new MqttMessage(new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 2),
                        MqttMessageIdVariableHeader.from(packetId)));
            } catch (Exception e) {
                mqttConnection.initiateClose("pubrec: exception=" + e);
            }
        }

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POPubRel po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        int packetId = ((MqttMessageIdVariableHeader) (po.getMessage().variableHeader())).messageId();
        Producer producer = producers.remove(packetId);
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + ", packetId=" + packetId + ", producer=" + producer);
            if (producer != null)
                producer.commit();
            mqttConnection.getOutboundQueue().enqueue(
                    new MqttMessage(new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_LEAST_ONCE, false, 2),
                            MqttMessageIdVariableHeader.from(packetId))
            );
        } catch (Exception e) {
            mqttConnection.initiateClose("pubrel: exception=" + e);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POPubComp po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        int packetId = ((MqttMessageIdVariableHeader) (po.getMessage().variableHeader())).messageId();
        removeReplay(packetId);
        POSendMessage poSendMessage = outboundPackets.remove(packetId);
        if (poSendMessage == null || poSendMessage.getQos() != MqttQoS.EXACTLY_ONCE)
            mqttConnection.getConnectionQueue().enqueue(new POProtocolError(po.getMessage()));
        else {
            try {
                if (!poSendMessage.getTransaction().isClosed()) {
                    poSendMessage.getTransaction().commit();
                    poSendMessage.getSubscription().restart();
                }
            } catch (Exception e) {
                mqttConnection.initiateClose("pubcomp: exception=" + e);
            }
        }

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POSubscribe po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");

        MqttSubscribeMessage subscribeMessage = po.getMessage();
        MqttMessageIdVariableHeader variableHeader = subscribeMessage.variableHeader();
        MqttSubscribePayload payload = subscribeMessage.payload();

        List<Integer> grantedQoS = new ArrayList<Integer>();
        List<MqttTopicSubscription> subscriptions = payload.topicSubscriptions();
        for (int i = 0; i < subscriptions.size(); i++) {
            grantedQoS.add(subscribe(subscriptions.get(i)));
        }
        MqttSubAckPayload subAckPayload = new MqttSubAckPayload(grantedQoS);
        mqttConnection.getOutboundQueue().enqueue(
                new MqttSubAckMessage(new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                        variableHeader, subAckPayload)
        );
        for (int i = 0; i < subscriptions.size(); i++) {
            try {
                List<MqttPublishMessage> retained = ctx.retainer.get(topicFilterTranslate(subscriptions.get(i).topicName()));
                for (int j = 0; j < retained.size(); j++) {
                    MqttPublishMessage publishMessage = retained.get(j);
                    mqttConnection.getOutboundQueue().enqueue(
                            new MqttPublishMessage(new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, true, 0),
                                    new MqttPublishVariableHeader(publishMessage.variableHeader().topicName(), 0), publishMessage.payload())
                    );
                }
            } catch (Exception e) {
                mqttConnection.initiateClose("subscribe: exception=" + e);
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POUnsubscribe po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");

        MqttUnsubscribeMessage unsubscribeMessage = po.getMessage();
        MqttMessageIdVariableHeader variableHeader = unsubscribeMessage.variableHeader();
        MqttUnsubscribePayload payload = unsubscribeMessage.payload();

        try {
            List<String> filters = payload.topics();
            for (int i = 0; i < filters.size(); i++) {
                unsubscribe(filters.get(i));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        mqttConnection.getOutboundQueue().enqueue(
                new MqttMessage(new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_LEAST_ONCE, false, 2),
                        variableHeader, null)
        );

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POCollect po) {
        if (connectionUsage == null)
            return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        for (Iterator<Map.Entry<String, Subscription>> iter = subscriptions.entrySet().iterator(); iter.hasNext(); ) {
            Subscription subscription = iter.next().getValue();
            Property receivedSecProp = connectionUsage.getEntity("subscriptions").getEntity(subscription.getTopicName()).getProperty("msgs-received");
            Property receivedTotalProp = connectionUsage.getEntity("subscriptions").getEntity(subscription.getTopicName()).getProperty("total-received");
            long received = subscription.getMsgsReceived();
            int totalReceived = subscription.getTotalMsgsReceived();
            double deltasec = Math.max(1.0, (double) (System.currentTimeMillis() - po.getLastCollect()) / 1000.0);
            double ssec = ((double) received / (double) deltasec) + 0.5;
            try {
                if (((Integer) receivedSecProp.getValue()).intValue() != ssec)
                    receivedSecProp.setValue(new Integer((int) ssec));
                if (((Integer) receivedTotalProp.getValue()).intValue() != totalReceived)
                    receivedTotalProp.setValue(new Integer(totalReceived));
            } catch (Exception e) {
            }
        }

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    @Override
    public String toString() {
        return (mqttConnection != null ? (mqttConnection.toString() + "/") : "") + "MQTTSession, " +
                "clientId='" + clientId + '\'' +
                ", persistent=" + persistent;
    }

    private class ReplayEntry {
        int packetid;
        MqttMessage message;

        public ReplayEntry(int packetid, MqttMessage message) {
            this.packetid = packetid;
            this.message = message;
        }
    }
}
