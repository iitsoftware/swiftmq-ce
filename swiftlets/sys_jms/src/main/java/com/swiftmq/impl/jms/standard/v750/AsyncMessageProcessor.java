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

package com.swiftmq.impl.jms.standard.v750;

import com.swiftmq.jms.smqp.v750.AsyncMessageDeliveryRequest;
import com.swiftmq.swiftlet.queue.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class AsyncMessageProcessor extends MessageProcessor {
    RegisterMessageProcessor registerRequest = null;
    RunMessageProcessor runRequest = null;
    Session session = null;
    SessionContext ctx = null;
    Consumer consumer = null;
    int consumerCacheSize = 0;
    int recoveryEpoche = 0;
    final AtomicInteger deliveryCount = new AtomicInteger();
    final AtomicBoolean valid = new AtomicBoolean(true);
    final AtomicInteger numberMessages = new AtomicInteger();
    final AtomicInteger lowWaterMark = new AtomicInteger();
    final AtomicLong maxBulkSize = new AtomicLong(-1);
    final AtomicBoolean started = new AtomicBoolean(false);

    public AsyncMessageProcessor(Session session, SessionContext ctx, Consumer consumer, int consumerCacheSize, int recoveryEpoche) {
        super(consumer.getSelector());
        this.session = session;
        this.ctx = ctx;
        this.consumer = consumer;
        this.consumerCacheSize = consumerCacheSize;
        this.recoveryEpoche = recoveryEpoche;
        setAutoCommit(consumer.isAutoCommit());
        setBulkMode(true);
        createBulkBuffer(consumerCacheSize);
        registerRequest = new RegisterMessageProcessor(this);
        runRequest = new RunMessageProcessor(this);
        lowWaterMark.set(session.getMyConnection().ctx.consumerCacheLowWaterMark);
        if (lowWaterMark.get() * 2 >= consumerCacheSize)
            lowWaterMark.set(0);
    }

    public long getMaxBulkSize() {
        return maxBulkSize.get();
    }

    public void setMaxBulkSize(long maxBulkSize) {
        this.maxBulkSize.set(maxBulkSize == -1 ? maxBulkSize : maxBulkSize * 1024);
    }

    public int getConsumerCacheSize() {
        return consumerCacheSize;
    }

    public void setConsumerCacheSize(int consumerCacheSize) {
        this.consumerCacheSize = consumerCacheSize;
    }

    public boolean isValid() {
        return valid.get() && !session.closed.get();
    }

    public void stop() {
        valid.set(false);
    }

    public void reset() {
        deliveryCount.set(0);
        valid.set(true);
    }

    public void processMessages(int numberMessages) {
        this.numberMessages.set(numberMessages);
        if (isValid()) {
            ctx.sessionLoop.submit(runRequest);
        }
    }

    public void processMessage(MessageEntry messageEntry) {
        throw new RuntimeException("Invalid method call, bulk mode is enabled!");
    }

    public void processException(Exception exception) {
        valid.set(!(exception instanceof QueueHandlerClosedException));
    }

    public boolean isStarted() {
        return started.get();
    }

    public void register() {
        if (!isValid())
            return;
        try {
            QueuePullTransaction t = consumer.getReadTransaction();
            if (t != null && !t.isClosed()) {
                try {
                    started.set(true);
                    //         t.unregisterMessageProcessor(this);
                    t.registerMessageProcessor(this);
                } catch (QueueTransactionClosedException e) {
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        if (!isValid())
            return;
        ctx.incMsgsReceived(numberMessages.get());
        deliveryCount.addAndGet(numberMessages.get());
        boolean restart = false;
        if (maxBulkSize.get() != -1) {
            maxBulkSize.addAndGet(-getCurrentBulkSize());
            restart = deliveryCount.get() >= consumerCacheSize - lowWaterMark.get() || maxBulkSize.get() <= 0;
        } else
            restart = deliveryCount.get() >= consumerCacheSize - lowWaterMark.get();
        MessageEntry[] buffer = getBulkBuffer();
        if (isAutoCommit()) {
            MessageEntry[] bulk = new MessageEntry[numberMessages.get()];
            System.arraycopy(buffer, 0, bulk, 0, numberMessages.get());
            AsyncMessageDeliveryRequest request = new AsyncMessageDeliveryRequest(consumer.getClientDispatchId(), consumer.getClientListenerId(), null, bulk, session.dispatchId, restart, recoveryEpoche);
            ctx.outboundLoop.submit(request);
        } else {
            for (int i = 0; i < numberMessages.get(); i++) {
                AsyncMessageDeliveryRequest request = new AsyncMessageDeliveryRequest(consumer.getClientDispatchId(), consumer.getClientListenerId(), buffer[i], null, session.dispatchId, i == numberMessages.get() - 1 && restart, recoveryEpoche);
                DeliveryItem item = new DeliveryItem();
                item.messageEntry = buffer[i];
                item.consumer = consumer;
                item.request = request;
                ctx.sessionLoop.submit(item);
            }
        }
        if (!restart) {
            ctx.sessionLoop.submit(registerRequest);
        } else {
            deliveryCount.set(0);
            maxBulkSize.set(-1);
            started.set(false);
        }
    }

    public String getDescription() {
        return session.toString() + "/AsyncMessageProcessor";
    }

    public String getDispatchToken() {
        return "none";
    }
}
