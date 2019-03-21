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

package com.swiftmq.impl.streams.processor;

import com.swiftmq.impl.streams.StreamContext;
import com.swiftmq.impl.streams.comp.io.Input;
import com.swiftmq.impl.streams.processor.po.POMessage;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.swiftlet.queue.*;

public class QueueMessageProcessor extends MessageProcessor {
    StreamContext ctx;
    Input input;
    QueueReceiver receiver;
    QueuePullTransaction transaction;
    MessageEntry messageEntry;
    boolean closed = false;

    public QueueMessageProcessor(StreamContext ctx, Input input, QueueReceiver receiver, Selector selector) {
        super(selector);
        this.ctx = ctx;
        this.input = input;
        this.receiver = receiver;
    }

    public void deregister() {
        if (closed)
            return;
        try {
            if (receiver != null)
                receiver.close();
            receiver = null;
        } catch (QueueException e) {
        }
        closed = true;
    }

    public void restart() throws Exception {
        transaction = receiver.createTransaction(false);
        transaction.registerMessageProcessor(this);
    }

    public QueuePullTransaction getTransaction() {
        return transaction;
    }

    public Input getInput() {
        return input;
    }

    public MessageImpl getMessage() {
        return messageEntry.getMessage();
    }

    @Override
    public void processMessage(MessageEntry messageEntry) {
        this.messageEntry = messageEntry;
        ctx.streamProcessor.dispatch(new POMessage(null, this));
    }

    @Override
    public void processException(Exception e) {
        e.printStackTrace();

    }

    @Override
    public boolean isValid() {
        return !closed;
    }

    @Override
    public String toString() {
        return "QueueMessageProcessor{" +
                "input=" + input +
                '}';
    }
}
