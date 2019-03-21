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
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.Property;
import com.swiftmq.mqtt.v311.MqttListener;
import com.swiftmq.mqtt.v311.netty.handler.codec.mqtt.*;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.auth.AuthenticationException;
import com.swiftmq.swiftlet.auth.ResourceLimitException;
import com.swiftmq.swiftlet.net.Connection;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.PipelineQueue;
import com.swiftmq.mqtt.v311.netty.buffer.ByteBuf;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MQTTConnection implements TimerListener, MqttListener, AssociateSessionCallback, MQTTVisitor {
    public static final String TP_CONNECTIONSVC = "sys$mqtt.connection.service";

    SwiftletContext ctx = null;
    Entity usage = null;
    Entity connectionTemplate = null;
    Connection connection = null;
    OutboundQueue outboundQueue = null;
    PipelineQueue connectionQueue = null;
    volatile ActiveLogin activeLogin = null;
    boolean closed = false;
    boolean closeInProgress = false;
    Lock closeLock = new ReentrantLock();
    String clientId = null;
    String username = "anonymous";
    String remoteHostname;
    boolean authenticated = false;
    int nConnectPackets = 0;
    boolean protocolInvalid = false;
    long keepaliveInterval = 0;
    int packetCount = 0;
    Will will = null;
    MqttConnectMessage connectMessage = null;
    MqttConnAckMessage connAckMessage = null;
    MqttConnectReturnCode rc = null;
    MQTTSession session = null;
    boolean cleanSession = false;
    String uniqueId;
    int uniqueIdCount = 0;
    boolean hasLastWill = false;
    String lastWillTopic = null;
    MqttQoS lastWillQoS = null;
    boolean lastWillRetain = false;
    ByteBuf lastWillPayload = null;
    Property receivedSecProp = null;
    Property sentSecProp = null;
    Property receivedTotalProp = null;
    Property sentTotalProp = null;

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
        connectionQueue = new PipelineQueue(ctx.threadpoolSwiftlet.getPool(TP_CONNECTIONSVC), "MQTTConnection", this);
        outboundQueue = new OutboundQueue(ctx, ctx.threadpoolSwiftlet.getPool(TP_CONNECTIONSVC), this);
        outboundQueue.startQueue();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + "/created");
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
        return activeLogin;
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
        if (uniqueIdCount == Integer.MAX_VALUE)
            uniqueIdCount = 0;
        else
            uniqueIdCount++;
        return uniqueId + "/" + uniqueIdCount;
    }

    public void collect(long lastCollect) {
        connectionQueue.enqueue(new POCollect(lastCollect));
    }

    @Override
    public MQTTConnection getMqttConnection() {
        return this;
    }

    @Override
    public void associated(MQTTSession session) {
        connectionQueue.enqueue(new POSessionAssociated(session));
    }

    @Override
    public synchronized void performTimeAction() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + "/performTimeAction, packetCount=" + packetCount);
        if (packetCount == 0)
            initiateClose("inactivity timeout");
        packetCount = 0;
    }

    private void process(MqttMessage message) {
        if (ctx.protSpace.enabled)
            ctx.protSpace.trace("mqtt", toString() + "/RCV: " + message);
        synchronized (this) {
            packetCount++;
        }
        if (message.fixedHeader() != null) {
            switch (message.fixedHeader().messageType()) {
                case CONNECT:
                    connectionQueue.enqueue(new POConnect((MqttConnectMessage) message));
                    break;
                case CONNACK:
                    connectionQueue.enqueue(new POProtocolError(message));
                    break;
                case SUBSCRIBE:
                    connectionQueue.enqueue(new POSubscribe((MqttSubscribeMessage) message));
                    break;
                case SUBACK:
                    connectionQueue.enqueue(new POProtocolError(message));
                    break;
                case UNSUBACK:
                    connectionQueue.enqueue(new POProtocolError(message));
                    break;
                case UNSUBSCRIBE:
                    connectionQueue.enqueue(new POUnsubscribe((MqttUnsubscribeMessage) message));
                    break;
                case PUBLISH:
                    connectionQueue.enqueue(new POPublish((MqttPublishMessage) message));
                    break;
                case PUBACK:
                    connectionQueue.enqueue(new POPubAck((MqttPubAckMessage) message));
                    break;
                case PUBREC:
                    connectionQueue.enqueue(new POPubRec(message));
                    break;
                case PUBREL:
                    connectionQueue.enqueue(new POPubRel(message));
                    break;
                case PUBCOMP:
                    connectionQueue.enqueue(new POPubComp(message));
                    break;
                case PINGREQ:
                    connectionQueue.enqueue(new POPingReq(message));
                    break;
                case PINGRESP:
                    connectionQueue.enqueue(new POProtocolError(message));
                    break;
                case DISCONNECT:
                    connectionQueue.enqueue(new PODisconnect(message));
                    break;
                default:
                    connectionQueue.enqueue(new POProtocolError(message));
            }
        } else {
            connectionQueue.enqueue(new POProtocolError(message));
        }
    }

    public void initiateClose(String reason) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + "/initiateClose, reason=" + reason);
        if (reason != null)
            ctx.logSwiftlet.logError(ctx.mqttSwiftlet.getName(), toString() + "/Disconnect client due to this reason: " + reason);
        ctx.networkSwiftlet.getConnectionManager().removeConnection(connection);
    }

    @Override
    public void onMessage(List<MqttMessage> list) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + "/onMessage, list=" + list);
        for (int i = 0; i < list.size(); i++) {
            process(list.get(i));
        }
    }

    @Override
    public void onException(Exception e) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + "/onException, exception=" + e);
        initiateClose(e.toString());
    }

    public void close() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", close ...");
        closeLock.lock();
        if (closeInProgress) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", close in progress, return");
            return;
        }
        //Todo: Remove Usage entry
        ctx.timerSwiftlet.removeTimerListener(this);
        closeInProgress = true;
        closeLock.unlock();
        Semaphore sem = new Semaphore();
        connectionQueue.enqueue(new POClose(sem));
        sem.waitHere();
        outboundQueue.stopQueue();
        closed = true;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", close done");
    }

    @Override
    public void visit(POConnect po) {
        if (closed || protocolInvalid) return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");

        if (nConnectPackets > 0) {
            protocolInvalid = true;
            initiateClose("protocol error, multiple connect packets");
            return;
        }
        nConnectPackets++;

        connectMessage = po.getMessage();
        MqttConnectVariableHeader variableConnectHeader = connectMessage.variableHeader();
        MqttConnectPayload payload = connectMessage.payload();
        clientId = payload.clientIdentifier();
        if (clientId == null || clientId.length() == 0) {
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
                if (password != null && pwd != null && password.equals(pwd) || password == pwd) {
                    rc = MqttConnectReturnCode.CONNECTION_ACCEPTED;
                    activeLogin = ctx.authSwiftlet.createActiveLogin(username, "MQTT");
                    activeLogin.setClientId(clientId);
                    authenticated = true;
                } else
                    throw new AuthenticationException("invalid password");
                keepaliveInterval = (long) ((double) (variableConnectHeader.keepAliveTimeSeconds() * 1000.0) * 1.5);
                if (keepaliveInterval > 0) {
                    ctx.timerSwiftlet.addTimerListener(keepaliveInterval, this);
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
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POSessionAssociated po) {
        if (closed || protocolInvalid) return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        session = po.getSession();
        connAckMessage.variableHeader().setSessionPresent(session.isWasPresent());
        outboundQueue.enqueue(connAckMessage);
        if (rc != MqttConnectReturnCode.CONNECTION_ACCEPTED) {
            protocolInvalid = true;
            initiateClose("connection not accepted");
        } else {
            uniqueId = "mqtt/" + SwiftletManager.getInstance().getRouterName() + "/" + clientId + "/" + System.currentTimeMillis();
            session.start();
        }
        session.fillConnectionUsage(usage.getEntity("MQTT Session"));
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POPublish po) {
        if (closed || protocolInvalid) return;
        session.visit(po);
    }

    @Override
    public void visit(POSendMessage po) {
        if (closed || protocolInvalid) return;
        session.visit(po);
    }

    @Override
    public void visit(POPubAck po) {
        if (closed || protocolInvalid) return;
        session.visit(po);
    }

    @Override
    public void visit(POPubRec po) {
        if (closed || protocolInvalid) return;
        session.visit(po);
    }

    @Override
    public void visit(POPubRel po) {
        if (closed || protocolInvalid) return;
        session.visit(po);
    }

    @Override
    public void visit(POPubComp po) {
        if (closed || protocolInvalid) return;
        session.visit(po);
    }

    @Override
    public void visit(POSubscribe po) {
        if (closed || protocolInvalid) return;
        session.visit(po);
    }

    @Override
    public void visit(POUnsubscribe po) {
        if (closed || protocolInvalid) return;
        session.visit(po);
    }

    @Override
    public void visit(POPingReq po) {
        if (closed || protocolInvalid) return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");

        outboundQueue.enqueue(new MqttMessage(new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0)));

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(PODisconnect po) {
        if (closed || protocolInvalid) return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");

        hasLastWill = false;

        initiateClose(null);

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POProtocolError po) {
        if (closed || protocolInvalid) return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        protocolInvalid = true;

        initiateClose("protocol error");

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POCollect po) {
        if (session == null)
            return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        long received = session.getMsgsReceived();
        long sent = session.getMsgsSent();
        int totalReceived = session.getTotalMsgsReceived();
        int totalSent = session.getTotalMsgsSent();
        session.visit(po);
        double deltasec = Math.max(1.0, (double) (System.currentTimeMillis() - po.getLastCollect()) / 1000.0);
        double rsec = ((double) received / (double) deltasec) + 0.5;
        double ssec = ((double) sent / (double) deltasec) + 0.5;
        try {
            if (((Integer) receivedSecProp.getValue()).intValue() != rsec)
                receivedSecProp.setValue(new Integer((int) rsec));
            if (((Integer) sentSecProp.getValue()).intValue() != ssec)
                sentSecProp.setValue(new Integer((int) ssec));
            if (((Integer) receivedTotalProp.getValue()).intValue() != totalReceived)
                receivedTotalProp.setValue(new Integer(totalReceived));
            if (((Integer) sentTotalProp.getValue()).intValue() != totalSent)
                sentTotalProp.setValue(new Integer(totalSent));
        } catch (Exception e) {
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    @Override
    public void visit(POClose po) {
        if (closed) return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
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
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    @Override
    public String toString() {
        return "MQTTConnection, clientid=" + clientId;
    }

    private class Will {
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
