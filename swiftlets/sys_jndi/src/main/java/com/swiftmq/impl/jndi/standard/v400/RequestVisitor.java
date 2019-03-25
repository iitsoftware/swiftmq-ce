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

package com.swiftmq.impl.jndi.standard.v400;

import com.swiftmq.impl.jndi.standard.SwiftletContext;
import com.swiftmq.jms.*;
import com.swiftmq.jndi.protocol.v400.*;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.jndi.JNDISwiftlet;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.swiftlet.queue.QueueSender;
import com.swiftmq.tools.dump.Dumpalizer;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.versioning.Versionable;
import com.swiftmq.tools.versioning.Versioned;

import javax.jms.Destination;
import java.io.Serializable;

public class RequestVisitor implements JNDIRequestVisitor {
    SwiftletContext ctx = null;
    QueueImpl replyToQueue = null;
    boolean forwardToTopic = false;

    public RequestVisitor(SwiftletContext ctx, QueueImpl replyToQueue, boolean forwardToTopic) {
        this.ctx = ctx;
        this.replyToQueue = replyToQueue;
        this.forwardToTopic = forwardToTopic;
    }

    public RequestVisitor(SwiftletContext ctx, QueueImpl replyToQueue) {
        this(ctx, replyToQueue, false);
    }

    private MessageImpl createReply(Object object) throws Exception {
        MessageImpl msg = null;
        if (object instanceof Versionable) {
            msg = new BytesMessageImpl();
            ((Versionable) object).transferToMessage((BytesMessageImpl) msg);
        } else {
            msg = new ObjectMessageImpl();
            ((ObjectMessageImpl) msg).setObject((Serializable) object);
        }
        msg.setJMSDeliveryMode(javax.jms.DeliveryMode.NON_PERSISTENT);
        msg.setJMSDestination(replyToQueue);
        msg.setJMSPriority(MessageImpl.MAX_PRIORITY);
        return msg;
    }

    private Versioned createVersioned(int version, JNDIRequest request) throws Exception {
        DataByteArrayOutputStream dos = new DataByteArrayOutputStream();
        Dumpalizer.dump(dos, request);
        return new Versioned(version, dos.getBuffer(), dos.getCount());
    }

    private BytesMessageImpl createMessage(Versionable versionable, Destination replyTo) throws Exception {
        BytesMessageImpl msg = new BytesMessageImpl();
        versionable.transferToMessage(msg);
        msg.setJMSDeliveryMode(javax.jms.DeliveryMode.NON_PERSISTENT);
        msg.setJMSDestination(new TopicImpl(JNDISwiftlet.JNDI_TOPIC));
        msg.setJMSPriority(MessageImpl.MAX_PRIORITY);
        if (replyTo != null)
            msg.setJMSReplyTo(replyTo);
        msg.setReadOnly(false);
        return msg;
    }

    public void visit(LookupRequest request) {
        String name = request.getName();
        Object object = ctx.jndiSwiftlet.getJNDIObject(name);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "visitor " + request + ": request '" + name + " returns: " + object);
        if (object == null) {
            if (forwardToTopic) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "visitor " + request + ": not found, forward to topic " + JNDISwiftlet.JNDI_TOPIC);
                try {
                    QueueSender sender = ctx.queueManager.createQueueSender(ctx.topicManager.getQueueForTopic(JNDISwiftlet.JNDI_TOPIC), null);
                    QueuePushTransaction transaction = sender.createTransaction();
                    Versionable versionable = new Versionable();
                    versionable.addVersioned(400, createVersioned(400, request), "com.swiftmq.jndi.protocol.v400.JNDIRequestFactory");
                    BytesMessageImpl msg = createMessage(versionable, replyToQueue);
                    transaction.putMessage(msg);
                    transaction.commit();
                    sender.close();
                } catch (Exception e1) {
                    e1.printStackTrace();
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "visitor " + request + ": exception occurred while forwarding lookup request: " + e1);
                }
            }
        } else {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "visitor " + request + ": replyTo " + replyToQueue);
            if (replyToQueue != null) {
                try {
                    QueueSender sender = ctx.queueManager.createQueueSender(replyToQueue.getQueueName(), null);
                    QueuePushTransaction transaction = sender.createTransaction();
                    MessageImpl reply = createReply(object);
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "visitor " + request + ": sending reply " + reply);
                    transaction.putMessage(reply);
                    transaction.commit();
                    sender.close();
                } catch (Exception e1) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "visitor " + request + ": exception occurred while sending reply: " + e1);
                }
            } else if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "visitor " + request + ": replyTo is not set. Unable to send.");
        }
    }

    public void visit(BindRequest request) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "visitor " + request);
        String name = request.getName();
        QueueImpl queue = request.getQueue();
        String msg = null;
        boolean registered = false;
        try {
            String queueName = queue.getQueueName();
            if (queueName.endsWith(SwiftletManager.getInstance().getRouterName())) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "visitor " + request + " is local");
                registered = true;
                name = name != null ? name : queueName;
                if (ctx.jndiSwiftlet.getJNDIObject(name) != null)
                    throw new Exception("bind failed, object '" + name + "' is already registered!");
                ctx.jndiSwiftlet.registerJNDIObject(name, queue);
            }
        } catch (Exception e) {
            msg = e.getMessage();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "visitor " + request + ": exception: " + msg);
        }
        if (registered) {
            if (replyToQueue != null) {
                try {
                    QueueSender sender = ctx.queueManager.createQueueSender(replyToQueue.getQueueName(), null);
                    QueuePushTransaction transaction = sender.createTransaction();
                    TextMessageImpl reply = new TextMessageImpl();
                    reply.setJMSDeliveryMode(javax.jms.DeliveryMode.NON_PERSISTENT);
                    reply.setJMSDestination(replyToQueue);
                    reply.setJMSPriority(MessageImpl.MAX_PRIORITY);
                    reply.setText(msg);
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "visitor " + request + ": sending reply " + reply);
                    transaction.putMessage(reply);
                    transaction.commit();
                    sender.close();
                } catch (Exception e1) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "visitor " + request + ": exception occurred while sending reply: " + e1);
                }
            } else if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "visitor " + request + ": replyTo is not set. Unable to send.");
        }
    }

    public void visit(RebindRequest request) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "visitor " + request);
        String name = request.getName();
        QueueImpl queue = request.getQueue();
        String msg = null;
        boolean registered = false;
        try {
            String queueName = queue.getQueueName();
            if (queueName.endsWith(SwiftletManager.getInstance().getRouterName())) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "visitor " + request + " is local");
                registered = true;
                if (ctx.jndiSwiftlet.getJNDIObject(name) != null)
                    throw new Exception("rebind failed, object '" + name + "' is already registered!");
                String oldName = ctx.jndiSwiftlet.getJNDIObjectName(queue);
                if (oldName != null) {
                    Object ro = ctx.jndiSwiftlet.getJNDIObject(oldName);
                    if (ro != null) {
                        if (ro.getClass() != queue.getClass())
                            throw new Exception("rebind failed; attempt to overwrite a different registration class!");
                        ctx.jndiSwiftlet.deregisterJNDIObject(oldName);
                    }
                }
                ctx.jndiSwiftlet.registerJNDIObject(name, queue);
            }
        } catch (Exception e) {
            msg = e.getMessage();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "visitor " + request + ": exception: " + msg);
        }
        if (registered) {
            if (replyToQueue != null) {
                try {
                    QueueSender sender = ctx.queueManager.createQueueSender(replyToQueue.getQueueName(), null);
                    QueuePushTransaction transaction = sender.createTransaction();
                    TextMessageImpl reply = new TextMessageImpl();
                    reply.setJMSDeliveryMode(javax.jms.DeliveryMode.NON_PERSISTENT);
                    reply.setJMSDestination(replyToQueue);
                    reply.setJMSPriority(MessageImpl.MAX_PRIORITY);
                    reply.setText(msg);
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "visitor " + request + ": sending reply " + reply);
                    transaction.putMessage(reply);
                    transaction.commit();
                    sender.close();
                } catch (Exception e1) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "visitor " + request + ": exception occurred while sending reply: " + e1);
                }
            } else if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "visitor " + request + ": replyTo is not set. Unable to send.");
        }
    }

    public void visit(UnbindRequest request) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.jndiSwiftlet.getName(), "visitor " + request);
        String name = request.getName();
        Object ro = ctx.jndiSwiftlet.getJNDIObject(name);
        if (ro != null) {
            if (ro.getClass() == TemporaryQueueImpl.class || ro.getClass() == TemporaryTopicImpl.class)
                ctx.jndiSwiftlet.deregisterJNDIObject(name);
        }
    }
}
