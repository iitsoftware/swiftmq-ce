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

package com.swiftmq.mgmt;

/**
 * A CommandExecutor is attached to a Command object and actually executes the commmand.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 * @see Command
 */
public interface CommandExecutor {

    /**
     * Called to execute the command.
     * This method is called from the MgmtSwiftlet when a user performs the command
     * with CLI or SwiftMQ Explorer. The <code>context</code> parameter contains the
     * current command context, that is, for example, <code>String[]{"sys$queuemanager","queues"}</code>
     * for "/sys$queuemanager/queues". The <code>entity</code> is the Entity object where the command
     * is attached to, and <code>parameter</code> are the parameters, given to this command.
     * For example, the command "new testqueue1 cache-size 200" will be translated into the
     * parameter <code>String[]{"new","testqueue1","cache-size","200"}</code>.<br><br>
     * This method has to validate the parameters and executes the command. It returns a
     * String array which is either null (means success) or the following structure:<br><br>
     * <ul><li>String[0] contains <code>"Error:"</code> if an error has occured or
     * <code>"Information:"</code> if an information should be displayed to the user.</li>
     * <li>String[1] contains the error resp. the info message</li></ul><br><br>
     * Examples:<br><br>
     * <ul><li><code>return new String[]{"Information:", "To activate this Change, a Reboot of this Router is required."};</code></li>
     * <li><code>return new String[]{"Error:", "Mandatory Property '" + p.getName() + "' must be set."};</code></li>
     * <li><code>return new String[]{"Error:", e.getMessage()};</code></li></ul>
     *
     * @param context   current context.
     * @param entity    parent entity.
     * @param parameter command parameter.
     * @return state structure.
     */
    public String[] execute(String[] context, Entity entity, String[] parameter);
}

