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
import com.swiftmq.impl.queue.standard.queue.MessageQueue;
import com.swiftmq.mgmt.Command;
import com.swiftmq.mgmt.CommandExecutor;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.TreeCommands;

public class Activate implements CommandExecutor {
    SwiftletContext ctx = null;

    public Activate(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    protected String _getCommand() {
        return "activate";
    }

    protected String _getPattern() {
        return "activate <queue>";
    }

    protected String _getDescription() {
        return "Toggles active/pause delivery of messages to consumers.";
    }

    public Command createCommand() {
        return new Command(_getCommand(), _getPattern(), _getDescription(), true, this, true, true);
    }

    @Override
    public String[] execute(String[] context, Entity entity, String[] cmd) {
        if (cmd.length != 2)
            return new String[]{TreeCommands.ERROR, "Invalid command, please try '" + _getPattern() + "'"};
        String queueName = cmd[1];
        MessageQueue queue = (MessageQueue) ctx.queueManager.getQueueForInternalUse(queueName);
        if (queue == null)
            return new String[]{TreeCommands.ERROR, "Queue not found: " + queueName};
        queue.activate(!queue.isActive());
        try {
            ctx.usageList.getEntity(queueName).getProperty("active").setValue(queue.isActive());
        } catch (Exception e) {
        }
        return null;
    }
}
