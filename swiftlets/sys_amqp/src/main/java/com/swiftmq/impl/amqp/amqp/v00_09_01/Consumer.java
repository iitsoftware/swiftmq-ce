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
import com.swiftmq.swiftlet.queue.*;

public class Consumer {
    SwiftletContext ctx;
    ChannelHandler channelHandler;
    String consumerTag;
    String originalQueueName;
    String queueName;
    boolean noAck;
    boolean noLocal;
    QueueReceiver receiver = null;
    QueuePullTransaction readTransaction = null;
    MessageProcessor messageProcessor = null;
    boolean closed = false;

    public Consumer(SwiftletContext ctx, ChannelHandler channelHandler, String consumerTag, String originalQueueName, String queueName, boolean noAck, boolean noLocal) throws Exception {
        this.ctx = ctx;
        this.consumerTag = consumerTag;
        this.channelHandler = channelHandler;
        this.originalQueueName = originalQueueName;
        this.queueName = queueName;
        this.noAck = noAck;
        this.noLocal = noLocal;
        receiver = ctx.queueManager.createQueueReceiver(queueName, channelHandler.getVersionedConnection().getActiveLogin(), null);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/created");
    }

    public ChannelHandler getChannelHandler() {
        return channelHandler;
    }

    public String getConsumerTag() {
        return consumerTag;
    }

    public String getOriginalQueueName() {
        return originalQueueName;
    }

    public String getQueueName() {
        return queueName;
    }

    public boolean isNoAck() {
        return noAck;
    }

    public boolean isNoLocal() {
        return noLocal;
    }

    public boolean isClosed() {
        return closed;
    }

    public void startMessageProcessor() throws QueueException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/startMessageProcessor");
        if (readTransaction == null)
            readTransaction = receiver.createTransaction(false);
        if (messageProcessor == null) {
            messageProcessor = new SourceMessageProcessor(ctx, this);
            readTransaction.registerMessageProcessor(messageProcessor);
        }
    }

    public void startMessageProcessor(SourceMessageProcessor messageProcessor) throws QueueException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/startMessageProcessor, messageProcessor=" + messageProcessor);
        this.messageProcessor = messageProcessor;
        readTransaction.registerMessageProcessor(messageProcessor);
    }

    public void ack(MessageIndex messageIndex) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/ack, messageIndex=" + messageIndex);
        QueuePullTransaction t = receiver.createTransaction(true);
        t.moveToTransaction(messageIndex, readTransaction);
        t.commit();
    }

    public void reject(MessageIndex messageIndex) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/reject, messageIndex=" + messageIndex);
        QueuePullTransaction t = receiver.createTransaction(true);
        t.moveToTransaction(messageIndex, readTransaction);
        t.rollback();
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/close");
        closed = true;
        try {
            if (readTransaction != null)
                readTransaction.rollback();
            readTransaction = null;
            if (receiver != null)
                receiver.close();
            receiver = null;
        } catch (Exception e) {
        }
    }

    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append("[Consumer");
        sb.append(" consumerTag='").append(consumerTag).append('\'');
        sb.append(", originalQueueName='").append(originalQueueName).append('\'');
        sb.append(", queueName='").append(queueName).append('\'');
        sb.append(", closed=").append(closed);
        sb.append(']');
        return sb.toString();
    }
}
