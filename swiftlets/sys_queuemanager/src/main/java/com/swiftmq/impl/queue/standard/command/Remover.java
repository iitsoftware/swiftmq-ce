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
import com.swiftmq.mgmt.Command;
import com.swiftmq.mgmt.CommandExecutor;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.TreeCommands;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.MessageIndex;

import java.util.Iterator;
import java.util.SortedSet;

public class Remover
        implements CommandExecutor {
    static String COMMAND = "remove";
    static String PATTERN = "remove <queue> (<message-key>|*)|(-index <start> <stop>)";
    static String DESCRIPTION = "Remove message(s) either by message-key or by message index.";
    SwiftletContext ctx = null;

    public Remover(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    public Command createCommand() {
        return new Command(COMMAND, PATTERN, DESCRIPTION, true, this, false, false);
    }

    public String[] execute(String[] context, Entity entity, String[] cmd) {
        if (cmd.length < 3 || cmd.length > 5)
            return new String[]{TreeCommands.ERROR, "Invalid command, please try '" + PATTERN + "'"};
        String[] result = null;
        try {
            if (!ctx.queueManager.isQueueDefined(cmd[1]))
                throw new Exception("Unknown queue: " + cmd[1]);
            AbstractQueue aq = (AbstractQueue) ctx.queueManager.getQueueForInternalUse(cmd[1]);
            if (!(aq instanceof MessageQueue))
                throw new Exception("Operation not supported on this type of queue!");
            MessageQueue mq = (MessageQueue) aq;
            SortedSet content = mq.getQueueIndex();
            if (cmd[2].equals("-index")) {
                if (cmd.length != 5)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'remove <queue> -index <start> <stop>'"};
                int start = Integer.parseInt(cmd[3]);
                int stop = Integer.parseInt(cmd[4]);
                if (stop < start)
                    throw new Exception("Stop index is less than start index.");
                int i = 0, cnt = 0;
                for (Iterator iter = content.iterator(); iter.hasNext(); ) {
                    MessageIndex mi = (MessageIndex) iter.next();
                    if (i >= start && i <= stop) {
                        try {
                            mq.removeMessageByIndex(mi);
                            cnt++;
                        } catch (MessageLockedException ignored) {
                        }
                    }
                    if (i > stop)
                        break;
                    i++;
                }
                return new String[]{TreeCommands.INFO, cnt + " messages removed."};
            } else {
                String key = cmd[2];
                int id = -1;
                if (!key.equals("*"))
                    id = Integer.parseInt(key);
                boolean found = false;
                for (Iterator iter = content.iterator(); iter.hasNext(); ) {
                    MessageIndex mi = (MessageIndex) iter.next();
                    if (id == -1 || mi.getId() == id) {
                        found = true;
                        mq.removeMessageByIndex(mi);
                        if (id != -1)
                            break;
                    }
                }
                if (!found && id != -1)
                    throw new Exception("Message key '" + id + "' not found!");
            }
        } catch (Exception e) {
            result = new String[]{TreeCommands.ERROR, e.getMessage()};
        }
        return result;
    }
}

