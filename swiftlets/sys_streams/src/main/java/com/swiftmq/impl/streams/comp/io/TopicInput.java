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

package com.swiftmq.impl.streams.comp.io;

import com.swiftmq.impl.streams.StreamContext;
import com.swiftmq.impl.streams.comp.message.Message;
import com.swiftmq.impl.streams.processor.QueueMessageProcessor;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.EntityRemoveException;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.queue.QueueReceiver;

/**
 * Consumes Messages from a Topic.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class TopicInput implements DestinationInput {
    StreamContext ctx;
    String name;
    String destinationName;
    String selector;
    Message current;
    InputCallback callback;
    QueueMessageProcessor messageProcessor;
    boolean durable = false;
    String clientId = null;
    String durableName = null;
    int subscriberId;
    String queueName;
    int msgsProcessed = 0;
    int totalMsg = 0;
    Entity usage = null;
    boolean started = false;

    TopicInput(StreamContext ctx, String name) {
        this.ctx = ctx;
        this.name = name;
        this.destinationName = name;
        try {
            EntityList inputList = (EntityList) ctx.usage.getEntity("inputs");
            usage = inputList.createEntity();
            usage.setName(name);
            usage.createCommands();
            inputList.addEntity(usage);
            usage.getProperty("01-type").setValue("Topic");
            usage.getProperty("destinationname").setValue(destinationName);
        } catch (Exception e) {
        }
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getSelector() {
        return selector;
    }

    @Override
    public QueueMessageProcessor getMessageProcessor() {
        return messageProcessor;
    }

    @Override
    public void setMessageProcessor(QueueMessageProcessor messageProcessor) {
        this.messageProcessor = messageProcessor;
    }

    @Override
    public DestinationInput destinationName(String destinationName) {
        this.destinationName = destinationName;
        try {
            usage.getProperty("destinationname").setValue(destinationName);
        } catch (Exception e) {
        }
        return this;
    }

    @Override
    public String destinationName() {
        return destinationName;
    }

    @Override
    public DestinationInput selector(String selector) {
        this.selector = selector;
        return this;
    }

    @Override
    public Input current(Message current) {
        this.current = current;
        return this;
    }

    @Override
    public Message current() {
        return current;
    }

    @Override
    public DestinationInput onInput(InputCallback callback) {
        this.callback = callback;
        return this;
    }

    @Override
    public void executeCallback() throws Exception {
        if (callback != null) {
            callback.execute(this);
        }
        msgsProcessed++;
    }

    /**
     * Internal use.
     */
    public boolean isDurable() {
        return durable;
    }

    /**
     * Internal use.
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Internal use.
     */
    public String getDurableName() {
        return durableName;
    }

    /**
     * Internal use.
     */
    public int getSubscriberId() {
        return subscriberId;
    }

    /**
     * Internal use.
     */
    public void setSubscriberId(int subscriberId) {
        this.subscriberId = subscriberId;
    }

    /**
     * Internal use.
     */
    public String getQueueName() {
        return queueName;
    }

    /**
     * Internal use.
     */
    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    /**
     * Marks this TopicInput as durable.
     *
     * @return TopicInput
     */
    public TopicInput durable() {
        durable = true;
        return this;
    }

    /**
     * Sets the client id for a durable subscriber.
     *
     * @param clientId client id
     * @return TopicInput
     */
    public TopicInput clientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    /**
     * Sets the durable name for a durable subscriber.
     *
     * @param durableName durable name
     * @return TopicInput
     */
    public TopicInput durableName(String durableName) {
        this.durableName = durableName;
        return this;
    }

    @Override
    public void collect(long interval) {
        if ((long) totalMsg + (long) msgsProcessed > Integer.MAX_VALUE)
            totalMsg = 0;
        totalMsg += msgsProcessed;
        try {
            usage.getProperty("input-total-processed").setValue(totalMsg);
            usage.getProperty("input-processing-rate").setValue((int) (msgsProcessed / ((double) interval / 1000.0)));
        } catch (Exception e) {
        }
        msgsProcessed = 0;
    }

    @Override
    public void start() throws Exception {
        if (started)
            return;
        if (!ctx.ctx.topicManager.isTopicDefined(destinationName))
            ctx.ctx.topicManager.createTopic(destinationName);
        MessageSelector ms = null;
        if (selector != null) {
            ms = new MessageSelector(selector);
            ms.compile();
        }
        if (durable) {
            ActiveLogin dlogin = ctx.ctx.authenticationSwiftlet.createActiveLogin(clientId, "DURABLE");
            dlogin.setClientId(clientId);
            TopicImpl topic = ctx.ctx.topicManager.verifyTopic(new TopicImpl(destinationName));
            queueName = ctx.ctx.topicManager.subscribeDurable(durableName, topic, ms, false, dlogin);
        } else {
            queueName = ctx.ctx.queueManager.createTemporaryQueue();
            subscriberId = ctx.ctx.topicManager.subscribe(destinationName, ms, false, queueName);
        }
        QueueReceiver receiver = ctx.ctx.queueManager.createQueueReceiver(queueName, (ActiveLogin) null, null);
        messageProcessor = new QueueMessageProcessor(ctx, this, receiver, null);
        messageProcessor.restart();
        started = true;
    }

    @Override
    public void close() {
        try {
            if (usage != null)
                ctx.usage.getEntity("inputs").removeEntity(usage);
        } catch (EntityRemoveException e) {
        }
        if (started) {
            messageProcessor.deregister();
            messageProcessor = null;
            if (!durable) {
                ctx.ctx.topicManager.unsubscribe(subscriberId);
                try {
                    ctx.ctx.queueManager.deleteTemporaryQueue(queueName);
                } catch (QueueException e) {

                }
            }
            ctx.stream.removeInput(this);
            started = false;
        }
    }

    @Override
    public String toString() {
        return "TopicInput{" +
                "name='" + name + '\'' +
                ", destinationName='" + destinationName + '\'' +
                ", selector='" + selector + '\'' +
                ", durable=" + durable +
                ", clientId='" + clientId + '\'' +
                ", durableName='" + durableName + '\'' +
                ", destinationName='" + queueName + '\'' +
                '}';
    }
}
