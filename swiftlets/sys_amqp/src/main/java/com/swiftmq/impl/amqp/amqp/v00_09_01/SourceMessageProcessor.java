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

package com.swiftmq.impl.amqp.amqp.v00_09_01;

import com.swiftmq.impl.amqp.SwiftletContext;
import com.swiftmq.impl.amqp.amqp.v00_09_01.po.POSendMessages;
import com.swiftmq.impl.amqp.amqp.v00_09_01.transformer.BasicOutboundTransformer;
import com.swiftmq.impl.amqp.amqp.v00_09_01.transformer.OutboundTransformer;
import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.swiftlet.queue.MessageProcessor;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SourceMessageProcessor extends MessageProcessor {
    private static final int MAX_BULKSIZE = 100;
    SwiftletContext ctx;
    Consumer consumer;
    MessageEntry messageEntry = null;
    final AtomicInteger numberMessages = new AtomicInteger();
    Exception exception = null;
    final AtomicBoolean stopped = new AtomicBoolean(false);
    DataByteArrayOutputStream dbos = new DataByteArrayOutputStream(8192);
    OutboundTransformer outboundTransformer = new BasicOutboundTransformer();

    public SourceMessageProcessor(SwiftletContext ctx, Consumer consumer) {
        this.ctx = ctx;
        this.consumer = consumer;
        setAutoCommit(false);
        setBulkMode(true);
        createBulkBuffer(MAX_BULKSIZE);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), this + "/created");
    }

    public void processMessages(int numberMessages) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), this + "/processMessages, numberMessages=" + numberMessages);
        this.numberMessages.set(numberMessages);
        if (isValid())
            consumer.getChannelHandler().dispatch(new POSendMessages(this));
    }

    public void processMessage(MessageEntry messageEntry) {
        this.messageEntry = messageEntry;
        if (isValid())
            consumer.getChannelHandler().dispatch(new POSendMessages(this));
    }

    public void processException(Exception e) {
        this.exception = e;
        if (isValid())
            consumer.getChannelHandler().dispatch(new POSendMessages(this));
    }

    public Exception getException() {
        return exception;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public Delivery[] getTransformedMessages() throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), this + "/getTransformedMessages ...");
        MessageEntry[] buffer = getBulkBuffer();
        Delivery[] deliveries = new Delivery[numberMessages.get()];
        for (int i = 0; i < numberMessages.get(); i++) {
            MessageEntry messageEntry = buffer[i];
            Delivery delivery = outboundTransformer.transform(messageEntry.getMessage());
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), this + "/getTransformedMessages, delivery=" + delivery);
            delivery.setRedelivered(messageEntry.getMessageIndex().getDeliveryCount() > 1);
            delivery.setMessageIndex(messageEntry.getMessageIndex());
            deliveries[i] = delivery;
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), this + "/getTransformedMessages, deliveries.length=" + deliveries.length);
        return deliveries;
    }

    public void setStopped(boolean stopped) {
        this.stopped.set(stopped);
    }

    public boolean isValid() {
        return !(consumer.isClosed() || stopped.get());
    }

    public String toString() {
        return consumer.toString() + "/SourceMessageProcessor";
    }
}
