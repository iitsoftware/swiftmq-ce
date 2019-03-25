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

package com.swiftmq.impl.amqp.amqp.v01_00_00;

import com.swiftmq.impl.amqp.SwiftletContext;
import com.swiftmq.impl.amqp.amqp.v01_00_00.po.POSendMessages;
import com.swiftmq.impl.amqp.amqp.v01_00_00.transformer.OutboundTransformer;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.swiftlet.queue.MessageIndex;
import com.swiftmq.swiftlet.queue.MessageProcessor;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import javax.jms.JMSException;

public class SourceMessageProcessor extends MessageProcessor {
    private static final int MAX_BULKSIZE = 500;
    SwiftletContext ctx;
    SourceLink sourceLink;
    int numberMessages = 0;
    MessageEntry messageEntry = null;
    Exception exception = null;
    boolean stopped = false;
    DataByteArrayOutputStream dbos = new DataByteArrayOutputStream(8192);
    DataByteArrayInputStream dbis = new DataByteArrayInputStream();

    public SourceMessageProcessor(SwiftletContext ctx, SourceLink sourceLink) {
        this.ctx = ctx;
        this.sourceLink = sourceLink;
        setAutoCommit(false);
        setBulkMode(true);
        createBulkBuffer(Math.min(MAX_BULKSIZE, (int) sourceLink.getLinkCredit()));
    }

    public SourceLink getSourceLink() {
        return sourceLink;
    }

    public void processMessages(int numberMessages) {
        this.numberMessages = numberMessages;
        if (isValid())
            sourceLink.getMySessionHandler().dispatch(new POSendMessages(this));
    }

    public void processMessage(MessageEntry messageEntry) {
        this.messageEntry = messageEntry;
        if (isValid())
            sourceLink.getMySessionHandler().dispatch(new POSendMessages(this));
    }

    public void processException(Exception exception) {
        this.exception = exception;
        if (isValid())
            sourceLink.getMySessionHandler().dispatch(new POSendMessages(this));
    }

    public Exception getException() {
        return exception;
    }

    private MessageImpl copyMessage(MessageImpl msg) throws Exception {
        dbos.rewind();
        msg.writeContent(dbos);
        dbis.reset();
        dbis.setBuffer(dbos.getBuffer(), 0, dbos.getCount());
        MessageImpl msgCopy = MessageImpl.createInstance(dbis.readInt());
        msgCopy.readContent(dbis);
        return msgCopy;
    }

    private Delivery transformMessage(MessageEntry messageEntry) throws JMSException {
        try {
            MessageImpl message = sourceLink.isQueue ? messageEntry.getMessage() : copyMessage(messageEntry.getMessage());
            MessageIndex messageIndex = messageEntry.getMessageIndex();
            OutboundTransformer transformer = sourceLink.getTransformer();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/transformMessage, transformer=" + transformer);
            if (transformer == null)
                throw new JMSException("No outbound message transformer found!");
            Delivery delivery = new Delivery(sourceLink, message, messageIndex);
            transformer.transform(delivery);
            return delivery;
        } catch (Exception e) {
            e.printStackTrace();
            throw new JMSException(e.toString());
        }
    }

    public Delivery getTransformedMessage() throws JMSException {
        return transformMessage(messageEntry);
    }

    public Delivery[] getTransformedMessages() throws JMSException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/getTransformedMessages ...");
        MessageEntry[] buffer = getBulkBuffer();
        Delivery[] deliveries = new Delivery[numberMessages];
        for (int i = 0; i < numberMessages; i++)
            deliveries[i] = transformMessage(buffer[i]);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/getTransformedMessages, deliveries.length=" + deliveries.length);
        return deliveries;
    }

    public void setStopped(boolean stopped) {
        this.stopped = stopped;
    }

    public boolean isValid() {
        return !(sourceLink.isClosed() || stopped);
    }

    public String toString() {
        return sourceLink.toString() + "/SourceMessageProcessor";
    }
}
