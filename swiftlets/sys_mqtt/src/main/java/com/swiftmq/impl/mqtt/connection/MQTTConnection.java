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

package com.swiftmq.impl.mqtt.connection;

import com.swiftmq.impl.mqtt.SwiftletContext;
import com.swiftmq.impl.mqtt.po.*;
import com.swiftmq.impl.mqtt.pubsub.Producer;
import com.swiftmq.impl.mqtt.session.AssociateSessionCallback;
import com.swiftmq.impl.mqtt.session.MQTTSession;
import com.swiftmq.impl.mqtt.v311.MqttListener;
import com.swiftmq.impl.mqtt.v311.netty.buffer.ByteBuf;
import com.swiftmq.impl.mqtt.v311.netty.handler.codec.mqtt.*;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.auth.AuthenticationException;
import com.swiftmq.swiftlet.auth.ResourceLimitException;
import com.swiftmq.swiftlet.net.Connection;
import com.swiftmq.swiftlet.threadpool.EventLoop;
import com.swiftmq.swiftlet.threadpool.EventProcessor;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.concurrent.AtomicWrappingCounterInteger;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.PipelineQueue;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class MQTTConnection implements TimerListener, MqttListener, AssociateSessionCallback, MQTTVisitor {
    SwiftletContext ctx = null;
    Entity usage = null;
    Entity connectionTemplate = null;
    Connection connection = null;
    OutboundQueue outboundQueue = null;
    PipelineQueue connectionQueue = null;
    final AtomicReference<ActiveLogin> activeLogin = new AtomicReference<>();
    final AtomicBoolean closed = new AtomicBoolean(false);
    final AtomicBoolean closeInProgress = new AtomicBoolean(false);
    String clientId = null;
    String username = "anonymous";
    String remoteHostname;
    final AtomicBoolean authenticated = new AtomicBoolean(false);
    final AtomicInteger nConnectPackets = new AtomicInteger();
    final AtomicBoolean protocolInvalid = new AtomicBoolean(false);
    final AtomicLong keepaliveInterval = new AtomicLong();
    final AtomicInteger packetCount = new AtomicInteger();
    Will will = null;
    MqttConnectMessage connectMessage = null;
    MqttConnAckMessage connAckMessage = null;
    MqttConnectReturnCode rc = null;
    MQTTSession session = null;
    boolean cleanSession = false;
    String uniqueId;
    AtomicWrappingCounterInteger uniqueIdCount = new AtomicWrappingCounterInteger(0);
    boolean hasLastWill = false;
    String lastWillTopic = null;
    MqttQoS lastWillQoS = null;
    boolean lastWillRetain = false;
    ByteBuf lastWillPayload = null;
    Property receivedSecProp = null;
    Property sentSecProp = null;
    Property receivedTotalProp = null;
    Property sentTotalProp = null;
    EventLoop eventLoop;

    public MQTTConnection(SwiftletContext ctx, Connection connection, Entity usage, Entity connectionTemplate) {
        this.ctx = ctx;
        this.connection = connection;
        this.usage = usage;
        this.connectionTemplate = connectionTemplate;
        remoteHostname = connection.getHostname();
        receivedSecProp = usage.getProperty("msgs-received");
        sentSecProp = usage.getProperty("msgs-sent");
        receivedTotalProp = usage.getProperty("total-received");
        sentTotalProp = usage.getProperty("total-sent");
        eventLoop = ctx.threadpoolSwiftlet.createEventLoop("sys$mqtt.connection.inbound", new EventProcessor() {
            @Override
            public void process(List<Object> list) {
                list.forEach(e -> ((POObject) e).accept(MQTTConnection.this));
            }
        });
        outboundQueue = new OutboundQueue(ctx, this);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + "/created");
    }

    public String getClientId() {
        return clientId;
    }

    public Entity getUsage() {
        return usage;
    }

    public Entity getConnectionTemplate() {
        return connectionTemplate;
    }

    public Connection getConnection() {
        return connection;
    }

    public ActiveLogin getActiveLogin() {
        return activeLogin.get();
    }

    public String getRemoteHostname() {
        return connection.getHostname();
    }

    public PipelineQueue getConnectionQueue() {
        return connectionQueue;
    }

    public OutboundQueue getOutboundQueue() {
        return outboundQueue;
    }

    public String nextId() {
        return uniqueId + "/" + uniqueIdCount.getAndIncrement();
    }

    public void collect(long lastCollect) {
        eventLoop.submit(new POCollect(lastCollect));
    }

    @Override
    public MQTTConnection getMqttConnection() {
        return this;
    }

    @Override
    public void associated(MQTTSession session) {
        eventLoop.submit(new POSessionAssociated(session));
    }

    @Override
    public void performTimeAction() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + "/performTimeAction, packetCount=" + packetCount.get());
        if (packetCount.getAndSet(0) == 0)
            initiateClose("inactivity timeout");
    }

    private void process(MqttMessage message) {
        if (ctx.protSpace.enabled)
            ctx.protSpace.trace("mqtt", this + "/RCV: " + message);
        packetCount.getAndIncrement();
        if (message.fixedHeader() != null) {
            switch (message.fixedHeader().messageType()) {
                case CONNECT:
                    eventLoop.submit(new POConnect((MqttConnectMessage) message));
                    break;
                case CONNACK:
                    eventLoop.submit(new POProtocolError(message));
                    break;
                case SUBSCRIBE:
                    eventLoop.submit(new POSubscribe((MqttSubscribeMessage) message));
                    break;
                case SUBACK:
                    eventLoop.submit(new POProtocolError(message));
                    break;
                case UNSUBACK:
                    eventLoop.submit(new POProtocolError(message));
                    break;
                case UNSUBSCRIBE:
                    eventLoop.submit(new POUnsubscribe((MqttUnsubscribeMessage) message));
                    break;
                case PUBLISH:
                    eventLoop.submit(new POPublish((MqttPublishMessage) message));
                    break;
                case PUBACK:
                    eventLoop.submit(new POPubAck((MqttPubAckMessage) message));
                    break;
                case PUBREC:
                    eventLoop.submit(new POPubRec(message));
                    break;
                case PUBREL:
                    eventLoop.submit(new POPubRel(message));
                    break;
                case PUBCOMP:
                    eventLoop.submit(new POPubComp(message));
                    break;
                case PINGREQ:
                    eventLoop.submit(new POPingReq(message));
                    break;
                case PINGRESP:
                    eventLoop.submit(new POProtocolError(message));
                    break;
                case DISCONNECT:
                    eventLoop.submit(new PODisconnect(message));
                    break;
                default:
                    eventLoop.submit(new POProtocolError(message));
            }
        } else {
            eventLoop.submit(new POProtocolError(message));
        }
    }

    public void initiateClose(String reason) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + "/initiateClose, reason=" + reason);
        if (reason != null)
            ctx.logSwiftlet.logError(ctx.mqttSwiftlet.getName(), this + "/Disconnect client due to this reason: " + reason);
        ctx.networkSwiftlet.getConnectionManager().removeConnection(connection);
    }

    @Override
    public void onMessage(List<MqttMessage> list) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + "/onMessage, list=" + list);
        for (MqttMessage mqttMessage : list) {
            process(mqttMessage);
        }
    }

    @Override
    public void onException(Exception e) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + "/onException, exception=" + e);
        initiateClose(e.toString());
    }

    public void close() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", close ...");
        if (closed.get() || closeInProgress.getAndSet(true)) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", closed or close in progress, return");
            return;
        }
        //Todo: Remove Usage entry
        ctx.timerSwiftlet.removeTimerListener(this);
        Semaphore sem = new Semaphore();
        eventLoop.submit(new POClose(sem));
        sem.waitHere();
        outboundQueue.close();
        closed.set(true);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", close done");
    }

    @Override
    public void visit(POConnect po) {
        if (closed.get() || protocolInvalid.get()) return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " ...");

        if (nConnectPackets.get() > 0) {
            protocolInvalid.set(true);
            initiateClose("protocol error, multiple connect packets");
            return;
        }
        nConnectPackets.getAndIncrement();

        connectMessage = po.getMessage();
        MqttConnectVariableHeader variableConnectHeader = connectMessage.variableHeader();
        MqttConnectPayload payload = connectMessage.payload();
        clientId = payload.clientIdentifier();
        if (clientId == null || clientId.isEmpty()) {
            if (!variableConnectHeader.isCleanSession())
                rc = MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED;
            else
                clientId = UUID.randomUUID().toString();
        }
        if (rc == null) {
            String password = null;
            if (variableConnectHeader.hasUserName())
                username = payload.userName();
            if (variableConnectHeader.hasPassword())
                password = new String(payload.passwordInBytes());
            try {
                ctx.authSwiftlet.verifyHostLogin(username, remoteHostname);
                String pwd = ctx.authSwiftlet.getPassword(username);
                if (Objects.equals(password, pwd)) {
                    rc = MqttConnectReturnCode.CONNECTION_ACCEPTED;
                    activeLogin.set(ctx.authSwiftlet.createActiveLogin(username, "MQTT"));
                    activeLogin.get().setClientId(clientId);
                    authenticated.set(true);
                } else
                    throw new AuthenticationException("invalid password");
                keepaliveInterval.set((long) (variableConnectHeader.keepAliveTimeSeconds() * 1000.0 * 1.5));
                if (keepaliveInterval.get() > 0) {
                    ctx.timerSwiftlet.addTimerListener(keepaliveInterval.get(), this);
                }
                if (variableConnectHeader.isWillFlag())
                    will = new Will(payload.willTopic(), variableConnectHeader.willQos(), variableConnectHeader.isWillRetain(), payload.willMessageInBytes());
                cleanSession = variableConnectHeader.isCleanSession();
                if (!cleanSession) {
                    ctx.sessionRegistry.associateSession(clientId, this);
                } else {
                    ctx.sessionRegistry.removeSession(clientId);
                    MQTTSession session = new MQTTSession(ctx, clientId, false);
                    session.associate(this);
                    associated(session);
                }
                hasLastWill = variableConnectHeader.isWillFlag();
                lastWillRetain = variableConnectHeader.isWillRetain();
                lastWillQoS = MqttQoS.valueOf(variableConnectHeader.willQos());
                lastWillTopic = payload.willTopic();
                lastWillPayload = new ByteBuf(payload.willMessageInBytes());
                try {
                    usage.getProperty("client-id").setValue(clientId);
                    usage.getProperty("username").setValue(username);
                    usage.getProperty("mqtt-protlevel").setValue(variableConnectHeader.version());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } catch (AuthenticationException e) {
                rc = MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD;
            } catch (ResourceLimitException e) {
                rc = MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED;
            }
        }

        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, connectMessage.fixedHeader().isDup(), connectMessage.fixedHeader().qosLevel(), connectMessage.fixedHeader().isRetain(), 2);
        MqttConnAckVariableHeader variableHeader = new MqttConnAckVariableHeader(rc, false);
        connAckMessage = new MqttConnAckMessage(fixedHeader, variableHeader);
        if (rc != MqttConnectReturnCode.CONNECTION_ACCEPTED) {
            protocolInvalid.set(true);
            initiateClose("not authenticated");
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POSessionAssociated po) {
        if (closed.get() || protocolInvalid.get()) return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " ...");
        session = po.getSession();
        connAckMessage.variableHeader().setSessionPresent(session.isWasPresent());
        outboundQueue.submit(connAckMessage);
        if (rc != MqttConnectReturnCode.CONNECTION_ACCEPTED) {
            protocolInvalid.set(true);
            initiateClose("connection not accepted");
        } else {
            uniqueId = "mqtt/" + SwiftletManager.getInstance().getRouterName() + "/" + clientId + "/" + System.currentTimeMillis();
            session.start();
        }
        session.fillConnectionUsage(usage.getEntity("MQTT Session"));
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POPublish po) {
        if (closed.get() || protocolInvalid.get()) return;
        session.visit(po);
    }

    @Override
    public void visit(POSendMessage po) {
        if (closed.get() || protocolInvalid.get()) return;
        session.visit(po);
    }

    @Override
    public void visit(POPubAck po) {
        if (closed.get() || protocolInvalid.get()) return;
        session.visit(po);
    }

    @Override
    public void visit(POPubRec po) {
        if (closed.get() || protocolInvalid.get()) return;
        session.visit(po);
    }

    @Override
    public void visit(POPubRel po) {
        if (closed.get() || protocolInvalid.get()) return;
        session.visit(po);
    }

    @Override
    public void visit(POPubComp po) {
        if (closed.get() || protocolInvalid.get()) return;
        session.visit(po);
    }

    @Override
    public void visit(POSubscribe po) {
        if (closed.get() || protocolInvalid.get()) return;
        session.visit(po);
    }

    @Override
    public void visit(POUnsubscribe po) {
        if (closed.get() || protocolInvalid.get()) return;
        session.visit(po);
    }

    @Override
    public void visit(POPingReq po) {
        if (closed.get() || protocolInvalid.get()) return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " ...");

        outboundQueue.submit(new MqttMessage(new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0)));

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(PODisconnect po) {
        if (closed.get() || protocolInvalid.get()) return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " ...");

        hasLastWill = false;

        initiateClose(null);

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POProtocolError po) {
        if (closed.get() || protocolInvalid.get()) return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " ...");
        protocolInvalid.set(true);

        initiateClose("protocol error");

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POCollect po) {
        if (session == null)
            return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " ...");
        long received = session.getMsgsReceived();
        long sent = session.getMsgsSent();
        int totalReceived = session.getTotalMsgsReceived();
        int totalSent = session.getTotalMsgsSent();
        session.visit(po);
        double deltasec = Math.max(1.0, (double) (System.currentTimeMillis() - po.getLastCollect()) / 1000.0);
        double rsec = ((double) received / deltasec) + 0.5;
        double ssec = ((double) sent / deltasec) + 0.5;
        try {
            if ((Integer) receivedSecProp.getValue() != rsec)
                receivedSecProp.setValue((int) rsec);
            if ((Integer) sentSecProp.getValue() != ssec)
                sentSecProp.setValue((int) ssec);
            if ((Integer) receivedTotalProp.getValue() != totalReceived)
                receivedTotalProp.setValue(totalReceived);
            if ((Integer) sentTotalProp.getValue() != totalSent)
                sentTotalProp.setValue(totalSent);
        } catch (Exception e) {
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POClose po) {
        if (closed.get()) return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " ...");
        if (hasLastWill) {
            try {
                String topicName = session.topicNameTranslate(lastWillTopic);
                Producer producer = new Producer(ctx, session, topicName);
                MqttPublishMessage msg = new MqttPublishMessage(new MqttFixedHeader(MqttMessageType.PUBLISH, false, lastWillQoS, lastWillRetain, 0),
                        new MqttPublishVariableHeader(lastWillTopic, 0), lastWillPayload);
                producer.send(msg);
                producer.commit();
                if (lastWillRetain)
                    ctx.retainer.add(topicName, msg);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (session != null) {
            session.stop();
            if (session.isPersistent()) {
                ctx.sessionRegistry.disassociateSession(clientId, session);
                if (cleanSession)
                    ctx.sessionRegistry.removeSession(clientId);
            } else
                session.destroy();
        }

        connectionQueue.close();
        po.setSuccess(true);
        if (po.getSemaphore() != null)
            po.getSemaphore().notifySingleWaiter();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", visit, po=" + po + " done");
    }

    @Override
    public String toString() {
        return "MQTTConnection, clientid=" + clientId;
    }

    private static class Will {
        String topic;
        int qos;
        boolean retain;
        byte[] message;

        Will(String topic, int qos, boolean retain, byte[] message) {
            this.topic = topic;
            this.qos = qos;
            this.retain = retain;
            this.message = message;
        }
    }
}
