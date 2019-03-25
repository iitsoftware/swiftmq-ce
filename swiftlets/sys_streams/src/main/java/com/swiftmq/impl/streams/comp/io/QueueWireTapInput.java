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
import com.swiftmq.impl.streams.processor.po.POWireTapMessage;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.EntityRemoveException;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.WireTapSubscriber;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Creates or re-uses a named WireTap on a Queue and adds a subscriber to it.
 * <p>
 * Messages are sent to the WireTap when the Message is inserted into the Queue.
 * If the WireTap has multiple subscribers (that is, Streams are using the same
 * WireTap name), Messages are distributed evenly over the subscribers in a
 * round-robin fashion.
 * </p>
 * <p>
 * Per default, a deep copy of a Message is performed before it is transferred to
 * a subscriber. So it is save to change it. If the Message will not be changed
 * by a subscriber, the deep copy can be disabled. But be careful! All changes
 * of a Message will have side effects.
 * </p>
 * <p>
 * Message transfer to a WireTap subscriber is done outside of a transaction. The
 * Subscriber uses a BlockingQueue to receive the Messages. The BlockingQueue has
 * a bufferSize which is 10 Messages per default. If the size is reached, the
 * Queue waits until there is free space. The time to wait is specified in
 * maxBlockTime with a default of 500 ms. If the timeout is reached, the Message
 * will not be inserted, so a WireTap subscriber may not receive all Messages
 * transferred through the Queue. For that reason, a WireTap is a good solution
 * if Message lost can be tolerated, e.g. in statistic scenarios.
 * </p>
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2017, All Rights Reserved
 */
public class QueueWireTapInput implements DestinationInput, WireTapSubscriber {
    StreamContext ctx;
    String destinationName;
    String name;
    String selector;
    Message current;
    InputCallback callback;
    int msgsProcessed = 0;
    int totalMsg = 0;
    Entity usage = null;
    boolean started = false;
    MessageSelector messageSelector = null;
    BlockingQueue<MessageImpl> queue = null;
    int bufferSize = 10;
    long maxBlockTime = 500;
    boolean requiresDeepCopy = true;

    QueueWireTapInput(StreamContext ctx, String destinationName, String name) {
        this.ctx = ctx;
        this.destinationName = destinationName;
        this.name = name;
        try {
            EntityList inputList = (EntityList) ctx.usage.getEntity("inputs");
            usage = inputList.createEntity();
            usage.setName(destinationName + "-" + name);
            usage.createCommands();
            inputList.addEntity(usage);
            usage.getProperty("01-type").setValue("QueueWireTapInput");
            usage.getProperty("destinationname").setValue(destinationName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public QueueMessageProcessor getMessageProcessor() {
        return null;
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

    /**
     * Sets the bufferSize of the internal BlockingQueue. Default is 10 Messages.
     *
     * @param bufferSize Buffer Size
     * @return this
     */
    public QueueWireTapInput bufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    /**
     * Sets the maximum block time when the internal BlockingQueue is full. Default is 500 ms. If the
     * time is reached, the Message will not be inserted.
     *
     * @param maxBlockTime Max Block Time
     * @return this
     */
    public QueueWireTapInput maxBlockTime(long maxBlockTime) {
        this.maxBlockTime = maxBlockTime;
        return this;
    }

    /**
     * Returns whether Message inserts requires a deep copy (default is true).
     *
     * @return deepCopy flag
     */
    @Override
    public boolean requieresDeepCopy() {
        return requiresDeepCopy;
    }

    /**
     * Sets whether the Message inserts requires a deep copy in order to change it later on.
     * Default is true.
     *
     * @param requiresDeepCopy deepCopy flag
     * @return this
     */
    public QueueWireTapInput requiresDeepCopy(boolean requiresDeepCopy) {
        this.requiresDeepCopy = requiresDeepCopy;
        return this;
    }

    /**
     * Internal use.
     */
    @Override
    public boolean isSelected(MessageImpl message) {
        return messageSelector == null || messageSelector.isSelected(message);
    }

    /**
     * Internal use.
     */
    @Override
    public void putMessage(MessageImpl message) {
        try {
            if (queue.offer(message, maxBlockTime, TimeUnit.MILLISECONDS))
                ctx.streamProcessor.dispatch(new POWireTapMessage(null, this));
        } catch (InterruptedException e) {

        }
    }

    /**
     * Internal use.
     */
    public Message next() {
        Message msg = null;
        try {
            msg = ctx.messageBuilder.wrap(queue.take());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return msg;
    }

    @Override
    public void setMessageProcessor(QueueMessageProcessor messageProcessor) {

    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Internal use.
     */
    public String getSelector() {
        return selector;
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
        if (selector != null) {
            messageSelector = new MessageSelector(selector);
            messageSelector.compile();
        }
        queue = new ArrayBlockingQueue<MessageImpl>(bufferSize);
        AbstractQueue queue = ctx.ctx.queueManager.getQueueForInternalUse(destinationName);
        if (queue == null)
            throw new Exception("Queue '" + destinationName + "' is undefined!");
        queue.addWireTapSubscriber(name, this);
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
            AbstractQueue queue = ctx.ctx.queueManager.getQueueForInternalUse(destinationName);
            if (queue != null)
                queue.removeWireTapSubscriber(name, this);
            started = false;
        }
        ctx.stream.removeInput(this);

    }

    @Override
    public String toString() {
        return "QueueWireTapInput{" +
                "destinationName='" + destinationName + '\'' +
                ", name='" + name + '\'' +
                ", selector='" + selector + '\'' +
                ", bufferSize=" + bufferSize +
                ", maxBlockTime=" + maxBlockTime +
                '}';
    }
}
