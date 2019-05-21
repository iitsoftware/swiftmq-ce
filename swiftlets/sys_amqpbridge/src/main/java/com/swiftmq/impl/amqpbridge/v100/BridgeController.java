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

package com.swiftmq.impl.amqpbridge.v100;

import com.swiftmq.amqp.v100.client.*;
import com.swiftmq.amqp.v100.generated.messaging.message_format.*;
import com.swiftmq.amqp.v100.generated.transport.definitions.DeliveryTag;
import com.swiftmq.amqp.v100.generated.transport.definitions.Milliseconds;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.AMQPBoolean;
import com.swiftmq.amqp.v100.types.AMQPUnsignedByte;
import com.swiftmq.impl.amqpbridge.SwiftletContext;
import com.swiftmq.impl.amqpbridge.accounting.AccountingProfile;
import com.swiftmq.impl.amqpbridge.accounting.BridgeCollector100;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.timer.event.TimerListener;

import java.util.Date;
import java.util.List;

public class BridgeController implements ExceptionListener, TimerListener, MessageAvailabilityListener {
    SwiftletContext ctx = null;
    Entity bridgeEntity = null;
    ConnectionHolder source = null;
    ConnectionHolder target = null;
    boolean enabled = false;
    boolean connected = false;
    long retryInterval;
    Entity usageEntity = null;
    BridgeDeliveryMemory bridgeDeliveryMemory = null;
    AccountingProfile accountingProfile = null;
    BridgeCollector100 bridgeCollector = null;

    public BridgeController(final SwiftletContext ctx, Entity bridgeEntity) {
        this.ctx = ctx;
        this.bridgeEntity = bridgeEntity;
        bridgeDeliveryMemory = new BridgeDeliveryMemory(ctx, toString());

        usageEntity = ((EntityList) ctx.usage.getEntity("bridges100")).createEntity();
        usageEntity.setName(bridgeEntity.getName());
        usageEntity.setDynamicObject(this);
        usageEntity.createCommands();
        try {
            ctx.usage.getEntity("bridges100").addEntity(usageEntity);
        } catch (EntityAddException e) {
        }

        Property prop = bridgeEntity.getProperty("enabled");
        enabled = (Boolean) prop.getValue();
        if (enabled)
            connect();
        prop.setPropertyChangeListener(new PropertyChangeListener() {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                enabled = (Boolean) newValue;
                if (enabled)
                    connect();
                else
                    disconnect();
            }
        });

        prop = bridgeEntity.getProperty("retryinterval");
        retryInterval = (Long) prop.getValue();
        if (retryInterval > 0)
            ctx.timerSwiftlet.addTimerListener(retryInterval, BridgeController.this);
        prop.setPropertyChangeListener(new PropertyChangeListener() {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                if (retryInterval > 0)
                    ctx.timerSwiftlet.removeTimerListener(BridgeController.this);
                retryInterval = (Long) newValue;
                if (retryInterval > 0)
                    ctx.timerSwiftlet.addTimerListener(retryInterval, BridgeController.this);
            }
        });
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/created");
    }

    private static int toIntQoS(String s) throws Exception {
        if (s.equals("at-least-once"))
            return QoS.AT_LEAST_ONCE;
        if (s.equals("at-most-once"))
            return QoS.AT_MOST_ONCE;
        if (s.equals("exactly-once"))
            return QoS.EXACTLY_ONCE;
        throw new Exception("Invalid QoS: " + s);
    }

    private void initBridgeCollector() {
        Entity sourceEntity = bridgeEntity.getEntity("source");
        bridgeCollector.setSourceHost((String) sourceEntity.getProperty("remote-hostname").getValue());
        bridgeCollector.setSourcePort(sourceEntity.getProperty("remote-port").getValue().toString());
        bridgeCollector.setSourceAddress((String) sourceEntity.getProperty("source-address").getValue());
        Entity targetEntity = bridgeEntity.getEntity("target");
        bridgeCollector.setTargetHost((String) targetEntity.getProperty("remote-hostname").getValue());
        bridgeCollector.setTargetPort(targetEntity.getProperty("remote-port").getValue().toString());
        bridgeCollector.setTargetAddress((String) targetEntity.getProperty("target-address").getValue());
    }

    private void createCollector() {
        bridgeCollector = new BridgeCollector100(bridgeEntity.getName());
        if (connected)
            initBridgeCollector();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), "createCollector, bridgeCollector=" + bridgeCollector);
    }

    public void startAccounting(AccountingProfile accountingProfile) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), "startAccounting, this.accountingProfile=" + this.accountingProfile);
        if (this.accountingProfile == null) {
            this.accountingProfile = accountingProfile;
            createCollector();
        }
    }

    public void flushAccounting() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), "flushAccounting, accountingProfile=" + accountingProfile + ", bridgeCollector=" + bridgeCollector);
        if (accountingProfile != null && bridgeCollector != null && bridgeCollector.isDirty())
            accountingProfile.getSource().send(bridgeCollector);
    }

    public void stopAccounting() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), "stopAccounting, accountingProfile=" + accountingProfile);
        if (accountingProfile != null) {
            accountingProfile = null;
            bridgeCollector = null;
        }
    }

    public void onException(Exception e) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/onException, e=" + e);
        disconnect();
    }

    public void performTimeAction() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/performTimeAction");
        if (!connected && enabled)
            connect();
    }

    public void messageAvailable(Consumer consumer) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/messageAvailable");
        ctx.poller.enqueue(this);
    }

    public synchronized void poll() {
        AMQPMessage msg = null;
        int n = 0;
        int tsize = 0;
        while (n < 10) {
            if (!connected)
                return;
            msg = source.getConsumer().receiveNoWait(this);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/poll, msg=" + msg);
            if (msg != null) {
                n++;
                tsize += msg.getBodySize();
                AMQPMessage toSend = new AMQPMessage();
                toSend.setHeader(msg.getHeader());
                toSend.setApplicationProperties(msg.getApplicationProperties());
                toSend.setMessageAnnotations(msg.getMessageAnnotations());
                toSend.setDeliveryAnnotations(msg.getDeliveryAnnotations());
                final Properties p = msg.getProperties();
                MessageIdIF messageIdIF = p.getMessageId();
                if (messageIdIF != null)
                    messageIdIF.accept(new MessageIdVisitorAdapter() {
                        public void visit(MessageIdString messageIdString) {
                            String s = messageIdString.getValue();
                            if (s != null && s.startsWith("ID:"))
                                p.setMessageId(new MessageIdString(s.substring(3)));
                        }
                    });
                toSend.setProperties(p);
                toSend.setFooter(msg.getFooter());
                if (msg.getAmqpValue() != null)
                    toSend.setAmqpValue(msg.getAmqpValue());
                else if (msg.getAmqpSequence() != null) {
                    List<AmqpSequence> l = msg.getAmqpSequence();
                    for (int i = 0; i < l.size(); i++)
                        toSend.addAmqpSequence(l.get(i));
                } else if (msg.getData() != null) {
                    List<Data> l = msg.getData();
                    for (int i = 0; i < l.size(); i++)
                        toSend.addData(l.get(i));
                }
                boolean persistent = true;
                int priority = 5;
                long ttl = -1;
                if (msg.getHeader() != null) {
                    Header header = msg.getHeader();
                    AMQPBoolean b = header.getDurable();
                    if (b != null)
                        persistent = b.getValue();
                    AMQPUnsignedByte prio = header.getPriority();
                    if (prio != null)
                        priority = prio.getValue();
                    Milliseconds ms = header.getTtl();
                    if (ms != null)
                        ttl = ms.getValue();
                }
                try {
                    if (!msg.isSettled()) {
                        // Use the incoming DeliveryTag to for the outgoing as well to match both messages
                        DeliveryTag dtag = msg.getDeliveryTag();
                        toSend.setDeliveryTag(dtag);
                        bridgeDeliveryMemory.addUnsettledSourceMessage(dtag, msg);
                    }
                    target.getProducer().send(toSend, persistent, priority, ttl);
                } catch (AMQPException e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/poll, exception=" + e);
                    disconnect();
                    break;
                }
            } else
                break;
        }
        BridgeCollector100 bc = bridgeCollector;
        if (bc != null)
            bc.incTotal(n, tsize);
        try {
            Property prop = usageEntity.getProperty("last-transfer-time");
            prop.setValue(new Date().toString());
            prop.setReadOnly(true);
            prop = usageEntity.getProperty("number-messages-transferred");
            int oldValue = (Integer) prop.getValue();
            if (oldValue + n < Integer.MAX_VALUE)
                oldValue += n;
            else
                oldValue = n;
            prop.setValue(new Integer(oldValue));
            prop.setReadOnly(true);
        } catch (Exception e) {
        }
        if (connected && msg != null)
            ctx.poller.enqueue(this);
    }

    private synchronized void connect() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/connect ...");
        try {
            int qos = toIntQoS((String) bridgeEntity.getProperty("transfer-qos").getValue());
            source = new ConnectionHolder(ctx, bridgeEntity.getEntity("source"), qos);
            target = new ConnectionHolder(ctx, bridgeEntity.getEntity("target"), qos);

            source.createConnection(this, null);
            target.createConnection(this, bridgeDeliveryMemory);
            connected = true;
            ctx.poller.enqueue(this);
            Property prop = usageEntity.getProperty("connecttime");
            prop.setValue(new Date().toString());
            prop.setReadOnly(true);

            if (bridgeCollector != null)
                initBridgeCollector();
            ctx.logSwiftlet.logInformation(ctx.bridgeSwiftlet.getName(), toString() + "/connected");
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/connect, exception=" + e);
            ctx.logSwiftlet.logError(ctx.bridgeSwiftlet.getName(), toString() + "/unable to connect, exception=" + e);
            if (source != null) {
                source.cancel();
                source = null;
            }
            if (target != null) {
                target.cancel();
                target = null;
            }
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/connect done");
    }

    private synchronized void disconnect() {
        if (!connected)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/disconnect ...");
        ctx.logSwiftlet.logInformation(ctx.bridgeSwiftlet.getName(), toString() + "/disconnecting");
        if (source != null) {
            source.close();
            source = null;
        }
        if (target != null) {
            target.close();
            target = null;
        }
        Property prop = usageEntity.getProperty("connecttime");
        try {
            prop.setValue("DISCONNECTED");
        } catch (Exception e) {
        }
        prop.setReadOnly(true);
        connected = false;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/disconnect done");
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/close");
        flushAccounting();
        enabled = false;
        disconnect();
        ctx.usage.getEntity("bridges-100").removeDynamicEntity(this);
        if (retryInterval > 0)
            ctx.timerSwiftlet.removeTimerListener(this);
    }

    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append("BridgeController");
        sb.append(" name=").append(bridgeEntity.getName());
        sb.append(", source=").append(source);
        sb.append(", target=").append(target);
        sb.append(", enabled=").append(enabled);
        sb.append(", connected=").append(connected);
        return sb.toString();
    }
}
