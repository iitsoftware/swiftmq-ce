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
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.EntityRemoveException;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.QueueReceiver;

/**
 * Consumes Messages from a Queue.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class QueueInput implements DestinationInput {
    StreamContext ctx;
    String name;
    String destinationName;
    String selector;
    Message current;
    InputCallback callback;
    QueueMessageProcessor messageProcessor;
    int msgsProcessed = 0;
    int totalMsg = 0;
    Entity usage = null;
    boolean started = false;

    QueueInput(StreamContext ctx, String name) {
        this.ctx = ctx;
        this.name = name;
        this.destinationName = name;
        try {
            EntityList inputList = (EntityList) ctx.usage.getEntity("inputs");
            usage = inputList.createEntity();
            usage.setName(name);
            usage.createCommands();
            inputList.addEntity(usage);
            usage.getProperty("atype").setValue("Queue");
            usage.getProperty("destinationname").setValue(destinationName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public QueueMessageProcessor getMessageProcessor() {
        return messageProcessor;
    }

    public void setMessageProcessor(QueueMessageProcessor messageProcessor) {
        this.messageProcessor = messageProcessor;
    }

    @Override
    public String getName() {
        return name;
    }

    public String getSelector() {
        return selector;
    }

    public DestinationInput selector(String selector) {
        this.selector = selector;
        return this;
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

    @Override
    public void collect(long interval) {
        if ((long) totalMsg + (long) msgsProcessed > Integer.MAX_VALUE)
            totalMsg = 0;
        totalMsg += msgsProcessed;
        try {
            usage.getProperty("input-total-processed").setValue(totalMsg);
            usage.getProperty("input-processing-rate").setValue((int) (msgsProcessed / ((double) interval / 1000.0)));
        } catch (Exception e) {
            e.printStackTrace();
        }
        msgsProcessed = 0;
    }

    @Override
    public void start() throws Exception {
        if (started)
            return;
        MessageSelector ms = null;
        if (selector != null) {
            ms = new MessageSelector(selector);
            ms.compile();
        }
        QueueReceiver receiver = ctx.ctx.queueManager.createQueueReceiver(destinationName, (ActiveLogin) null, ms);
        messageProcessor = new QueueMessageProcessor(ctx, this, receiver, ms);
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
            if (messageProcessor != null) {
                messageProcessor.deregister();
                messageProcessor = null;
            }
            started = false;
        }
        ctx.stream.removeInput(this);
    }

    @Override
    public String toString() {
        return "QueueInput{" +
                "name='" + name + '\'' +
                ", destinationName='" + destinationName + '\'' +
                ", selector='" + selector + '\'' +
                '}';
    }
}
