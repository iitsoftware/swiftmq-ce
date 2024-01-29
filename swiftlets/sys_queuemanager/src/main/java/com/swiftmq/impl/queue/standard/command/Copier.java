/*
 * Copyright 2024 IIT Software GmbH
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

package com.swiftmq.impl.queue.standard.command;

import com.swiftmq.impl.queue.standard.SwiftletContext;
import com.swiftmq.impl.queue.standard.queue.MessageLockedException;
import com.swiftmq.impl.queue.standard.queue.MessageQueue;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.mgmt.Command;
import com.swiftmq.mgmt.CommandExecutor;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.TreeCommands;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.util.SortedSet;

public class Copier
        implements CommandExecutor {
    SwiftletContext ctx = null;
    boolean remove = false;

    public Copier(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    protected String _getCommand() {
        return "copy";
    }

    protected String _getPattern() {
        return "copy <source> -queue|-topic <target> [(-selector <selector>)|(-index <start> <stop>)] [-maxlimit <nmsgs>]";
    }

    protected String _getDescription() {
        return "Copy messages.";
    }

    public Command createCommand() {
        return new Command(_getCommand(), _getPattern(), _getDescription(), true, this, false, false);
    }

    protected void setRemove(boolean remove) {
        this.remove = remove;
    }

    private int getMaxLimit(String[] cmd) throws Exception {
        int max = Integer.MAX_VALUE;
        for (int i = 0; i < cmd.length; i++) {
            if (cmd[i].equals("-maxlimit")) {
                if (i + 1 < cmd.length) {
                    max = Integer.valueOf(cmd[i + 1]);
                    break;
                } else
                    throw new Exception("Missing <nmsgs> for -maxlimit parameter!");
            }
        }
        return max;
    }

    private MessageImpl copyMessage(MessageImpl msg) throws Exception {
        DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();
        DataByteArrayInputStream dbis = new DataByteArrayInputStream();
        msg.writeContent(dbos);
        dbis.reset();
        dbis.setBuffer(dbos.getBuffer(), 0, dbos.getCount());
        MessageImpl msgCopy = MessageImpl.createInstance(dbis.readInt());
        msgCopy.readContent(dbis);
        return msgCopy;
    }

    private String[] copyWithSelector(String[] cmd) throws Exception {
        int max = getMaxLimit(cmd);
        int decr = max == Integer.MAX_VALUE ? 0 : 2;
        StringBuffer b = new StringBuffer();
        for (int i = 5; i < cmd.length - decr; i++) {
            if (i > 5)
                b.append(' ');
            b.append(cmd[i]);
        }
        MessageSelector selector = new MessageSelector(b.toString());
        selector.compile();
        QueueReceiver receiver = ctx.queueManager.createQueueReceiver(cmd[1], null, selector);
        QueuePullTransaction pullTx = receiver.createTransaction(false);
        QueueSender sender = null;
        QueueImpl targetQueueAddr = null;
        if (cmd[2].equals("-queue")) {
            sender = ctx.queueManager.createQueueSender(cmd[3], null);
            targetQueueAddr = new QueueImpl(cmd[3]);
        } else {
            String qft = ctx.topicManager.getQueueForTopic(cmd[3]);
            sender = ctx.queueManager.createQueueSender(qft, null);
            targetQueueAddr = new TopicImpl(cmd[3]);
        }
        QueuePushTransaction pushTx = sender.createTransaction();
        int cnt = 0;
        try {
            MessageEntry entry = null;
            while ((entry = pullTx.getMessage(0, selector)) != null && cnt < max) {
                MessageImpl msg = copyMessage(entry.getMessage());
                msg.setJMSDestination(targetQueueAddr);
                msg.setSourceRouter(null);
                msg.setDestRouter(null);
                pushTx.putMessage(msg);
                cnt++;
                pushTx.commit();
                if (remove) {
                    pullTx.commit();
                    pullTx = receiver.createTransaction(false);
                }
                pushTx = sender.createTransaction();
            }
        } finally {
            try {
                pullTx.rollback();
            } catch (Exception e) {
            }
            try {
                receiver.close();
            } catch (Exception e) {
            }
            try {
                pushTx.rollback();
            } catch (Exception e) {
            }
            try {
                sender.close();
            } catch (Exception e) {
            }
        }
        return new String[]{TreeCommands.INFO, cnt + " messages processed."};
    }

    public String[] execute(String[] context, Entity entity, String[] cmd) {
        if (cmd.length < 3 || !cmd[2].equals("-queue") && !cmd[2].equals("-topic"))
            return new String[]{TreeCommands.ERROR, "Invalid command, please try '" + _getPattern() + "'"};
        String[] result = null;
        try {
            if (cmd.length >= 6 && cmd[4].equals("-selector"))
                result = copyWithSelector(cmd);
            else {
                if (!ctx.queueManager.isQueueDefined(cmd[1]))
                    throw new Exception("Unknown queue: " + cmd[1]);
                AbstractQueue aq = ctx.queueManager.getQueueForInternalUse(cmd[1]);
                if (!(aq instanceof MessageQueue))
                    throw new Exception("Operation not supported on this type of queue!");
                MessageQueue sourceQueue = (MessageQueue) aq;
                QueueSender sender = null;
                QueueImpl targetQueueAddr = null;
                if (cmd[2].equals("-queue")) {
                    sender = ctx.queueManager.createQueueSender(cmd[3], null);
                    targetQueueAddr = new QueueImpl(cmd[3]);
                } else {
                    String qft = ctx.topicManager.getQueueForTopic(cmd[3]);
                    sender = ctx.queueManager.createQueueSender(qft, null);
                    targetQueueAddr = new TopicImpl(cmd[3]);
                }
                SortedSet content = sourceQueue.getQueueIndex();
                int max = getMaxLimit(cmd);
                int start = 0;
                int stop = Integer.MAX_VALUE;
                if (cmd.length >= 7 && cmd[4].equals("-index")) {
                    start = Integer.parseInt(cmd[5]);
                    stop = Integer.parseInt(cmd[6]);
                    if (stop < start)
                        throw new Exception("Stop index is less than start index.");
                } else {
                    if (cmd.length != 4 && max == Integer.MAX_VALUE)
                        return new String[]{TreeCommands.ERROR, "Invalid command, please try '" + _getCommand() + " <source> -queue|-topic <target> -index <start> <stop>'"};
                }
                int i = 0, cnt = 0;
                for (Object o : content) {
                    MessageIndex mi = (MessageIndex) o;
                    if (i >= start && i <= stop) {
                        try {
                            MessageImpl msg = copyMessage(sourceQueue.getMessageByIndex(mi).getMessage());
                            msg.setJMSDestination(targetQueueAddr);
                            msg.setSourceRouter(null);
                            msg.setDestRouter(null);
                            QueuePushTransaction t = sender.createTransaction();
                            t.putMessage(msg);
                            t.commit();
                            cnt++;
                            if (remove)
                                sourceQueue.removeMessageByIndex(mi);
                            if (cnt == max)
                                break;
                        } catch (MessageLockedException ignored) {
                        }
                    }
                    if (i > stop)
                        break;
                    i++;
                }
                return new String[]{TreeCommands.INFO, cnt + " messages processed."};
            }
        } catch (Exception e) {
            result = new String[]{TreeCommands.ERROR, e.getMessage()};
        }
        return result;
    }
}
