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

package com.swiftmq.impl.amqpbridge.v091;

import com.rabbitmq.client.*;
import com.swiftmq.impl.amqpbridge.SwiftletContext;
import com.swiftmq.impl.amqpbridge.accounting.BridgeCollector091;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.timer.event.TimerListener;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionHolder implements TimerListener {
    SwiftletContext ctx = null;
    Entity connectionEntity = null;
    Connection connection = null;
    Channel channel = null;
    String connectURI = null;
    String exchangeName = null;
    String exchangeType = null;
    String queueName = null;
    String routingKey = null;
    String consumerTag = null;
    boolean queueDurable = false;
    boolean queueExclusive = false;
    boolean queueAutodelete = false;
    AtomicInteger count = new AtomicInteger(0);
    Entity usageEntity = null;
    BridgeCollector091 bridgeCollector091 = null;

    public ConnectionHolder(SwiftletContext ctx, Entity connectionEntity) {
        this.ctx = ctx;
        this.connectionEntity = connectionEntity;
        connectURI = (String) connectionEntity.getProperty("connect-uri").getValue();
    }

    public void setUsageEntity(Entity usageEntity) {
        this.usageEntity = usageEntity;
        ctx.timerSwiftlet.addTimerListener(1000, this);
    }

    public void setBridgeCollector091(BridgeCollector091 bridgeCollector091) {
        this.bridgeCollector091 = bridgeCollector091;
    }

    public void createConnection() throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/createConnection ...");
        // Create the connection
        ConnectionFactory cfconn = new ConnectionFactory();
        cfconn.setUri(connectURI);
        connection = cfconn.newConnection();

        // Create Channel
        channel = connection.createChannel();

        exchangeName = (String) connectionEntity.getProperty("exchange-name").getValue();
        if (exchangeName == null)
            exchangeName = "";
        routingKey = (String) connectionEntity.getProperty("routing-key").getValue();
        if (routingKey == null)
            routingKey = "";
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/createConnection done");
    }

    public void createAndRunConnection(final ConnectionHolder sink, final ExceptionListener exceptionListener) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/createAndRunConnection ...");
        createConnection();

        exchangeType = (String) connectionEntity.getProperty("exchange-type").getValue();
        consumerTag = (String) connectionEntity.getProperty("consumer-tag").getValue();
        if (consumerTag == null)
            consumerTag = "";
        queueName = (String) connectionEntity.getProperty("queue-name").getValue();
        queueDurable = (Boolean) connectionEntity.getProperty("queue-option-durable").getValue();
        queueExclusive = (Boolean) connectionEntity.getProperty("queue-option-exclusive").getValue();
        queueAutodelete = (Boolean) connectionEntity.getProperty("queue-option-autodelete").getValue();
        if (exchangeName.length() > 0) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/createAndRunConnection, exchangeDeclare, exchangeName=" + exchangeName);
            channel.exchangeDeclare(exchangeName, exchangeType);
        }
        AMQP.Queue.DeclareOk declareOk = queueName != null ? channel.queueDeclare(queueName, queueDurable, queueExclusive, queueAutodelete, null) : channel.queueDeclare();
        if (exchangeName.length() > 0) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/createAndRunConnection, queueBind, exchangeName=" + exchangeName + ", queueName=" + declareOk.getQueue() + ", routingKey=" + routingKey);
            channel.queueBind(declareOk.getQueue(), exchangeName, routingKey);
        }
        consumerTag = channel.basicConsume(declareOk.getQueue(), false, consumerTag, new DefaultConsumer(channel) {
            public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/handleShutdownSignal, sig=" + sig);
                exceptionListener.onException(sig);
            }

            public void handleCancel(String consumerTag) throws IOException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/handleCancel");
                exceptionListener.onException(new Exception("Consumer cancelled"));
            }

            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/handleDelivery, consumerTag=" + consumerTag + ", envelope=" + envelope + ", properties=" + properties + ", body=" + (body == null ? "null" : body.length));
                try {
                    String msgId = properties.getMessageId();
                    if (msgId != null && msgId.startsWith("ID:"))
                        properties.setMessageId(msgId.substring(3));
                    sink.send(properties, body);
                    channel.basicAck(envelope.getDeliveryTag(), true);
                } catch (Exception e) {
                    exceptionListener.onException(e);
                }
            }
        });
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/createAndRunConnection done");
    }

    public void send(AMQP.BasicProperties props, byte[] body) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/send, properties=" + props + ", body=" + (body == null ? "null" : body.length));
        BridgeCollector091 bc = bridgeCollector091;
        if (bc != null)
            bc.incTotal(1, body.length);
        channel.basicPublish(exchangeName, routingKey, props, body);
        count.incrementAndGet();
    }

    public void performTimeAction() {
        if (count.get() == 0)
            return;
        try {
            Property prop = usageEntity.getProperty("last-transfer-time");
            prop.setValue(new Date().toString());
            prop.setReadOnly(true);
            prop = usageEntity.getProperty("number-messages-transferred");
            int oldValue = (Integer) prop.getValue();
            int n = count.getAndSet(0);
            if (oldValue + n < Integer.MAX_VALUE)
                oldValue += n;
            else
                oldValue = n;
            prop.setValue(new Integer(oldValue));
            prop.setReadOnly(true);
        } catch (Exception e) {
        }
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/close");
        if (usageEntity != null)
            ctx.timerSwiftlet.removeTimerListener(this);
        if (connection != null) {
            try {
                connection.close();
                connection = null;
            } catch (IOException e) {
            }
        }
    }

    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append("[ConnectionHolder");
        sb.append(" connectURI='").append(connectURI).append('\'');
        sb.append(", exchangeName='").append(exchangeName).append('\'');
        if (exchangeType != null)
            sb.append(", exchangeType='").append(exchangeType).append('\'');
        if (queueName != null)
            sb.append(", queueName='").append(queueName).append('\'');
        sb.append(", routingKey='").append(routingKey).append('\'');
        if (consumerTag != null)
            sb.append(", consumerTag='").append(consumerTag).append('\'');
        sb.append(']');
        return sb.toString();
    }
}
