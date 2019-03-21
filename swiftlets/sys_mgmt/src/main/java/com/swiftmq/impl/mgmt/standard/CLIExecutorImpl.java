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

import com.swiftmq.impl.mgmt.standard.auth.AuthenticatorImpl;
import com.swiftmq.impl.mgmt.standard.auth.Role;
import com.swiftmq.mgmt.Authenticator;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.RouterConfiguration;
import com.swiftmq.mgmt.TreeCommands;
import com.swiftmq.swiftlet.mgmt.CLIExecutor;
import com.swiftmq.util.SwiftUtilities;

public class CLIExecutorImpl implements CLIExecutor {
    SwiftletContext ctx;
    String context = null;
    Authenticator authenticator = null;

    CLIExecutorImpl(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    private String[] executeInternalCommand(String command, String lCtx) {
        String[] result;
        if (authenticator != null)
            result = RouterConfiguration.Singleton().executeInternalCommand(SwiftUtilities.tokenize(lCtx, "/"), SwiftUtilities.tokenize(command, " "), authenticator);
        else
            result = RouterConfiguration.Singleton().executeInternalCommand(SwiftUtilities.tokenize(lCtx, "/"), SwiftUtilities.tokenize(command, " "));
        return result;
    }

    private String[] executeCommand(String command) throws Exception {
        String[] result;
        if (authenticator != null)
            result = RouterConfiguration.Singleton().executeCommand(SwiftUtilities.tokenize(context, "/"), SwiftUtilities.parseCLICommand(command), authenticator);
        else
            result = RouterConfiguration.Singleton().executeCommand(SwiftUtilities.tokenize(context, "/"), SwiftUtilities.parseCLICommand(command));
        return result;
    }

    public void setAdminRole(String name) throws Exception {
        Entity roleEntity = ctx.root.getEntity("roles").getEntity(name);
        if (roleEntity != null)
            authenticator = new AuthenticatorImpl(ctx, new Role(ctx, roleEntity));
        else
            throw new Exception("Unknown admin role: " + name + "! Please declare it in the Management Swiftlet!");

    }

    public void execute(String command) throws Exception {
        String[] tcmd = SwiftUtilities.parseCLICommand(command);
        if (tcmd[0].equals(TreeCommands.CHANGE_CONTEXT)) {
            if (tcmd.length != 2)
                throw new Exception("Missing context in " + TreeCommands.CHANGE_CONTEXT + " command: " + command);
            context = SwiftUtilities.concat(SwiftUtilities.cutFirst(tcmd), "/");
        } else {
            String[] res = executeCommand(command);
            if (res != null) {
                if (res[0].equals(TreeCommands.ERROR))
                    throw new Exception(SwiftUtilities.concat(SwiftUtilities.cutFirst(res), " "));
            }
        }
    }

    private boolean isInternalCommand(String c) {
        return c.equals(TreeCommands.DIR_CONTEXT) || c.equals(TreeCommands.GET_CONTEXT_ENTITIES) || c.equals(TreeCommands.GET_CONTEXT_PROP);
    }

    public String getContext() {
        return context == null || context.endsWith("/") ? "" : "/" + context;
    }

    private Entity getContextEntity(Entity actEntity, String[] context, int stacklevel) {
        if (actEntity == null)
            return null;
        if (actEntity.getName().equals(context[stacklevel])) {
            if (stacklevel == context.length - 1)
                return actEntity;
            else
                return getContextEntity(actEntity.getEntity(context[stacklevel + 1]), context, stacklevel + 1);
        }
        return null;
    }

    private String determineContext(String c) {
        String[] rCtx = SwiftUtilities.tokenize(context, "/");
        if (c.equals("..")) {
            if (rCtx.length > 1)
                rCtx = SwiftUtilities.cutLast(rCtx);
            else {
                rCtx = new String[]{"/"};
            }
        } else if (c.equals("/")) {
            rCtx = new String[]{"/"};
        } else {
            if (c.startsWith("/"))
                rCtx = SwiftUtilities.tokenize(c, "/");
            else
                rCtx = SwiftUtilities.append(rCtx, SwiftUtilities.tokenize(c, "/"));

        }
        return SwiftUtilities.concat(rCtx, "/");
    }

    public String[] executeWithResult(String command) throws Exception {
        if (context == null)
            context = "/";
        String[] result = null;
        String[] tcmd = SwiftUtilities.tokenize(command, " ");
        if (tcmd[0].equals(TreeCommands.CHANGE_CONTEXT)) {
            if (tcmd.length != 2)
                throw new Exception("Missing context in " + TreeCommands.CHANGE_CONTEXT + " command: " + command);
            String c = determineContext(tcmd[1]);
            if (authenticator == null || c.equals("/"))
                context = c;
            else {
                String[] tc = SwiftUtilities.tokenize(c, "/");
                Entity entity = getContextEntity(RouterConfiguration.Singleton().getEntity(tc[0]), tc, 0);
                if (entity == null)
                    throw new Exception("Invalid context: " + c);
                if (authenticator.isContextGranted(entity))
                    context = c;
                else
                    throw new Exception("Context is not granted: " + c);
            }
        } else {
            if (isInternalCommand(tcmd[0])) {
                String lCtx = context;
                if (tcmd[0].equals(TreeCommands.DIR_CONTEXT) && tcmd.length == 2)
                    lCtx = determineContext(tcmd[1]);
                result = executeInternalCommand(command, lCtx);
            } else
                result = executeCommand(command);
            if (result != null) {
                if (result[0].equals(TreeCommands.ERROR))
                    throw new Exception(SwiftUtilities.concat(SwiftUtilities.cutFirst(result), " "));
            }
        }
        return result;
    }
}

