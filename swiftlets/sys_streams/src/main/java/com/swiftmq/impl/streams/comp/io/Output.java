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
import com.swiftmq.impl.streams.TransactionFinishListener;
import com.swiftmq.impl.streams.comp.message.Message;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.swiftlet.queue.QueueSender;

import javax.jms.Destination;

/**
 * Represents an Output to a Queue or Topic.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public abstract class Output {
    StreamContext ctx;
    String name;
    QueueSender sender;
    Destination dest;
    Entity usage;
    int sent = 0;
    volatile int txSent = 0;
    boolean closed = false;
    boolean dirty = false;

    Output(StreamContext ctx, String name) throws Exception {
        this.ctx = ctx;
        this.name = name;
        if (name != null) {
            try {
                EntityList outputList = (EntityList) ctx.usage.getEntity("outputs");
                usage = outputList.getEntity(name);
                usage = outputList.createEntity();
                usage.setName(name);
                usage.createCommands();
                outputList.addEntity(usage);
                usage.getProperty("atype").setValue(getType());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Returns the name of the Output
     *
     * @return name
     */
    public String name() {
        return name;
    }

    /**
     * Internal use.
     */
    public boolean isDirty() {
        return dirty;
    }

    /**
     * Internal use.
     */
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    protected abstract void setDestination(Destination destination);

    protected abstract String getType();

    protected abstract QueueSender createSender() throws Exception;

    protected abstract Destination getDestination() throws Exception;

    /**
     * Sends a Message.
     *
     * @param message Message
     * @return Output
     * @throws Exception
     */
    public Output send(Message message) throws Exception {
        try {
            if (sender == null) {
                sender = createSender();
                dest = getDestination();
            }
            txSent++;
            MessageImpl impl = message.getImpl();
            impl.setJMSDestination(dest);
            impl.setJMSTimestamp(System.currentTimeMillis());
            impl.setJMSMessageID(ctx.nextId());
            QueuePushTransaction transaction = sender.createTransaction();
            transaction.putMessage(impl);
            ctx.addTransaction(transaction, new TransactionFinishListener() {
                @Override
                public void transactionFinished() {
                    txSent--;
                    if (closed) {
                        try {
                            if (sender != null) {
                                if (txSent == 0) {
                                    sender.close();
                                }
                            }
                        } catch (QueueException e) {
                            ctx.logStackTrace(e);
                        }
                    }
                }
            });
            if (sent + 1 == Integer.MAX_VALUE)
                sent = 1;
            else
                sent++;
        } catch (Exception e) {
            if (!getDestination().toString().startsWith("tmp$"))
                throw e;
        }
        dirty = true;
        return this;
    }

    /**
     * Internal use.
     */
    public void collect(long interval) {
        if (usage == null)
            return;
        try {
            Property msgSent = usage.getProperty("messages-sent");
            msgSent.setValue(sent);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Closes this Output.
     */
    public void close() {
        closed = true;
        try {
            if (usage != null) {
                ctx.usage.getEntity("outputs").removeEntity(usage);
                ctx.stream.removeOutput(name);
            }
        } catch (Exception e) {
        }
    }
}
