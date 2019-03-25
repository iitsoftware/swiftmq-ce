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

package com.swiftmq.impl.queue.standard;

import com.swiftmq.mgmt.Command;
import com.swiftmq.mgmt.CommandExecutor;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.TreeCommands;
import com.swiftmq.swiftlet.queue.AbstractQueue;

public class Resetter
        implements CommandExecutor {
    static String COMMAND = "reset";
    static String PATTERN = "reset <queue>";
    static String DESCRIPTION = "Resets the consumed/produced counter to zero";
    SwiftletContext ctx = null;

    public Resetter(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    public Command createCommand() {
        return new Command(COMMAND, PATTERN, DESCRIPTION, true, this, true, true);
    }

    public String[] execute(String[] context, Entity entity, String[] cmd) {
        if (cmd.length < 2)
            return new String[]{TreeCommands.ERROR, "Invalid command, please try 'reset <queue>"};
        String[] result = null;
        try {
            String queueName = cmd[1];
            if (!ctx.queueManager.isQueueDefined(queueName))
                throw new Exception("Queue '" + queueName + "' is undefined!");
            AbstractQueue ac = ctx.queueManager.getQueueForInternalUse(queueName);
            if (ac != null)
                ac.resetCounters();
            else
                throw new Exception("Queue '" + queueName + "' not found!");
        } catch (Exception e) {
            result = new String[]{TreeCommands.ERROR, e.getMessage()};
        }
        return result;
    }
}