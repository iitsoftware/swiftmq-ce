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

package com.swiftmq.impl.mgmt.standard;

import com.swiftmq.jms.MessageCloner;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.TextMessageImpl;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.tools.concurrent.AtomicWrappingCounterInteger;
import com.swiftmq.tools.util.IdGenerator;
import com.swiftmq.util.SwiftUtilities;

import javax.jms.JMSException;
import java.util.concurrent.atomic.AtomicBoolean;

public class MessageInterfaceListener extends MessageProcessor {
    static final String PROP_REPLY_QUEUE = "JMS_SWIFTMQ_MGMT_REPLY_QUEUE";
    static final String PROP_SHOW_CMD_RESULT = "JMS_SWIFTMQ_MGMT_SHOW_COMMANDS_IN_RESULT";
    static final String PROP_SUBJECT = "JMS_SWIFTMQ_MGMT_SUBJECT";
    static final String TP_LISTENER = "sys$mgmt.listener.messageinterface";
    SwiftletContext ctx = null;
    ThreadPool myTP = null;
    String queueName = null;
    QueueReceiver receiver = null;
    QueuePullTransaction pullTransaction = null;
    final AtomicBoolean closed = new AtomicBoolean(false);
    MessageEntry entry = null;
    String idPrefix = null;
    AtomicWrappingCounterInteger cnt = new AtomicWrappingCounterInteger(1);
    String routerName = null;

    public MessageInterfaceListener(SwiftletContext ctx) throws Exception {
        this.ctx = ctx;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/creating ...");
        myTP = ctx.threadpoolSwiftlet.getPool(TP_LISTENER);
        idPrefix = newId();
        routerName = SwiftletManager.getInstance().getRouterName();
        queueName = (String) ctx.root.getEntity("message-interface").getProperty("request-queue-name").getValue();
        if (!ctx.queueManager.isQueueDefined(queueName))
            ctx.queueManager.createQueue(queueName, (ActiveLogin) null);
        receiver = ctx.queueManager.createQueueReceiver(queueName, null, null);
        pullTransaction = receiver.createTransaction(false);
        pullTransaction.registerMessageProcessor(this);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/creating done");
    }

    public boolean isValid() {
        return !closed.get();
    }

    public void processMessage(MessageEntry entry) {
        this.entry = entry;
        myTP.dispatchTask(this);
    }

    public void processException(Exception e) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/processException: " + e);
    }

    public String getDispatchToken() {
        return TP_LISTENER;
    }

    public String getDescription() {
        return ctx.mgmtSwiftlet.getName() + "/" + toString();
    }

    public void stop() {
    }

    private String newId() {
        return IdGenerator.getInstance().nextId('/') + "/" + SwiftletManager.getInstance().getRouterName() + "/javamailbridge/" + System.currentTimeMillis() + "/";
    }

    private String nextId() {
        return idPrefix + cnt.getAndIncrement();
    }

    private QueueImpl getReplyQueue(MessageImpl msg)
            throws JMSException {
        QueueImpl queue = (QueueImpl) msg.getJMSReplyTo();
        if (queue == null) {
            if (msg.propertyExists(PROP_REPLY_QUEUE)) {
                String name = msg.getStringProperty(PROP_REPLY_QUEUE);
                queue = new QueueImpl(name);
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/getReplyQueue: " + queue);
        return queue;
    }

    private String executeCommands(String commands, boolean showCommands) {
        String result = "No valid commands found.";
        boolean authenticated = !ctx.authEnabled;
        CLIExecutorImpl executor = new CLIExecutorImpl(ctx);
        if (commands != null) {
            String[] cmd = SwiftUtilities.tokenize(commands, "\n\r");
            if (cmd != null && cmd.length > 0) {
                StringBuffer b = new StringBuffer();
                for (int i = 0; i < cmd.length; i++) {
                    String s = cmd[i].trim();
                    if (s.length() > 0) {
                        try {
                            if (s.startsWith("authenticate")) {
                                String[] st = SwiftUtilities.tokenize(s, " ");
                                if (st.length != 2)
                                    throw new Exception("Expecting: authenticate <password>");
                                authenticated = ctx.password != null && st[1].equals(ctx.password);
                                if (!authenticated)
                                    throw new Exception("Invalid password!");
                                continue;
                            }
                            if (!authenticated)
                                throw new Exception("Not authenticated! First command must be: authenticate <password>");
                            if (showCommands)
                                b.append(routerName + executor.getContext() + "> " + s + "\n");
                            if (s.equals("exit"))
                                break;
                            if (ctx.traceSpace.enabled)
                                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/executing: " + s);
                            String[] r = executor.executeWithResult(s);
                            if (r != null)
                                b.append("\n" + SwiftUtilities.concat(SwiftUtilities.cutFirst(r), "\n") + "\n\n");
                        } catch (Exception e) {
                            if (ctx.traceSpace.enabled)
                                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/exception executing command '" + s + "': " + e.getMessage());
                            b.append("\n" + "Exception executing command '" + s + "': " + e.getMessage() + "\n");
                            break;
                        }
                    }
                }
                result = b.toString();
            }
        }
        return result;
    }

    private void sendReply(QueueImpl queue, MessageImpl request, String result) throws Exception {
        QueueSender sender = ctx.queueManager.createQueueSender(queue.getQueueName(), null);
        QueuePushTransaction tx = sender.createTransaction();
        TextMessageImpl replyMsg = (TextMessageImpl) MessageCloner.cloneMessage(request);
        if (request.propertyExists(PROP_SUBJECT))
            replyMsg.setStringProperty(PROP_SUBJECT, "Result for '" + request.getStringProperty(PROP_SUBJECT) + "'");
        replyMsg.setJMSDestination(queue);
        replyMsg.setJMSTimestamp(System.currentTimeMillis());
        replyMsg.setJMSMessageID(nextId());
        replyMsg.setJMSPriority(request.getJMSPriority());
        replyMsg.setJMSDeliveryMode(request.getJMSDeliveryMode());
        replyMsg.setText(result);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/sendReply: " + replyMsg);
        tx.putMessage(replyMsg);
        tx.commit();
        sender.close();
    }

    public void run() {
        try {
            pullTransaction.commit();
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/run, exception committing tx: " + e + ", exiting");
            return;
        }
        try {
            MessageImpl request = entry.getMessage();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/run, new message: " + request);
            String result = null;
            boolean showCommands = !request.propertyExists(PROP_SHOW_CMD_RESULT) || request.getBooleanProperty(PROP_SHOW_CMD_RESULT);
            if (request instanceof TextMessageImpl)
                result = executeCommands(((TextMessageImpl) request).getText(), showCommands);
            else
                result = "Invalid message type (" + request.getClass().getName() + "). Expect TextMessage!";

            QueueImpl queue = getReplyQueue(request);
            if (queue != null)
                sendReply(queue, request, result);
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/run, exception during processing: " + e);
            ctx.logSwiftlet.logError(ctx.mgmtSwiftlet.getName(), toString() + "/run, exception during processing: " + e);
        }
        if (closed.get())
            return;
        try {
            pullTransaction = receiver.createTransaction(false);
            pullTransaction.registerMessageProcessor(this);
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/run, exception creating new tx: " + e + ", exiting");
            return;
        }
    }

    public void close() {
        closed.set(true);
        try {
            receiver.close();
            ctx.queueManager.deleteQueue(queueName, true);
        } catch (Exception ignored) {
        }
    }

    public String toString() {
        return "MessageInterfaceListener";
    }
}