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

package com.swiftmq.impl.streams.comp.management;

import com.swiftmq.impl.streams.StreamContext;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.mgmt.CLIExecutor;
import com.swiftmq.util.SwiftUtilities;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

/**
 * Executor class for CLI commands. Note that you need to change to
 * the respective context before executing a command on it ("cc context").
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class CLI {
    DecimalFormat formatter = new DecimalFormat("###,##0.00", new DecimalFormatSymbols(Locale.US));

    StreamContext ctx;
    CLIExecutor executor;
    boolean exceptionsOn = true;

    /**
     * Internal use.
     */
    public CLI(StreamContext ctx) {
        this.ctx = ctx;
        executor = ctx.ctx.mgmtSwiftlet.createCLIExecutor();
    }

    /**
     * Sets the name of the admin role to use.
     *
     * @param name
     * @return this
     * @throws Exception
     */
    public CLI adminRole(String name) throws Exception {
        executor.setAdminRole(name);
        return this;
    }

    /**
     * Tells CLI to throw exceptions when they occur on execute().
     *
     * @return CLI
     */
    public CLI exceptionOn() {
        exceptionsOn = true;
        return this;
    }

    /**
     * Tells CLI to don't throw exceptions when they occur on execute().
     * This is useful if, e.g. a "new queue" command is executed but the
     * queue is already defined.
     *
     * @return CLI
     */
    public CLI exceptionOff() {
        exceptionsOn = false;
        return this;
    }

    /**
     * Returns the display name of a Property. The context must point to an EntityList. For example,
     * to get the display name of the "messagecount" Property of a queue, the context must be:
     * "sys$queuemanager/usage/messagecount".
     *
     * @param context CLI context
     * @return display name or "unknown"
     */
    public String displayName(String context) {
        String[] c = SwiftUtilities.tokenize(context, "/");
        Entity entity = (Entity) RouterConfiguration.Singleton().getContext(null, SwiftUtilities.cutLast(c), 0);
        if (entity == null)
            return "unknown";
        if (entity instanceof EntityList) {
            Property p = ((EntityList) entity).getTemplate().getProperty(c[c.length - 1]);
            if (p != null)
                return p.getDisplayName();
        }
        return "unknown";
    }

    /**
     * Returns a String array with the names of all entities. The context must point to an EntityList. For example,
     * to get the names of all defined queues, the context must be:
     * "sys$queuemanager/queues".
     *
     * @param context CLI context
     * @return names
     */
    public String[] entityNames(String context) {
        String[] result = null;
        String[] c = SwiftUtilities.tokenize(context, "/");
        Entity entity = (Entity) RouterConfiguration.Singleton().getContext(null, c, 0);
        if (entity == null)
            return result;
        if (entity instanceof EntityList) {
            result = entity.getEntityNames();
        }
        return result == null ? new String[]{} : result;
    }

    /**
     * Returns the JSON output of the CLI context which must point to an Entity or EntityList.
     *
     * @param context CLI context
     * @return JSON
     */
    public String contextJson(String context) {
        if (context.equals("/"))
            return RouterConfiguration.Singleton().toJson();
        String[] c = SwiftUtilities.tokenize(context, "/");
        Entity entity = (Entity) RouterConfiguration.Singleton().getContext(null, c, 0);
        if (entity == null)
            return null;
        return entity.toJson();
    }

    /**
     * Return the value of a Property.
     *
     * @param context  CLI context
     * @param propName Property name
     * @return value as String
     * @throws Exception
     */
    public String propertyValue(String context, String propName) throws Exception {
        String[] c = SwiftUtilities.tokenize(context, "/");
        Entity entity = (Entity) RouterConfiguration.Singleton().getContext(null, c, 0);
        if (entity == null)
            throw new Exception("Invalid context: " + context);
        Property prop = entity.getProperty(propName);
        if (prop == null)
            throw new Exception("Unknown property: " + propName + " in context: " + context);
        return prop.getValue() == null ? "" : prop.getValue().toString();
    }

    /**
     * Return the sum of a Property of an EntityList.
     *
     * @param context  CLI context
     * @param propName Property name
     * @return sum
     * @throws Exception
     */
    public long sumProperty(String context, String propName) throws Exception {
        String[] c = SwiftUtilities.tokenize(context, "/");
        Entity entity = (Entity) RouterConfiguration.Singleton().getContext(null, c, 0);
        if (entity == null)
            throw new Exception("Invalid context: " + context);
        Map entities = entity.getEntities();
        if (entities != null) {
            long result = 0;
            for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
                Entity e = (Entity) ((Map.Entry) iter.next()).getValue();
                Property prop = e.getProperty(propName);
                if (prop == null)
                    throw new Exception("Unknown property: " + propName + " in context: " + context);
                if (prop.getValue() != null) {
                    if (prop.getType() == Integer.class)
                        result += (Integer) prop.getValue();
                    else if (prop.getType() == Long.class)
                        result += (Long) prop.getValue();
                    else if (prop.getType() == Double.class)
                        result += Math.round(((Double) prop.getValue()));
                    else if (prop.getType() == Float.class)
                        result += Math.round(((Float) prop.getValue()));
                    else if (prop.getType() == String.class)
                        result += Math.round(formatter.parse((String) prop.getValue()).doubleValue());
                }
            }
            return result;
        }
        return 0;
    }

    /**
     * Executes a CLI command on the local router's management tree.
     *
     * @param command CLI command
     * @return CLI
     * @throws Exception If an exception occurs and exceptionOn() is set (default)
     */
    public CLI execute(String command) throws Exception {
        try {
            executor.execute(command);
        } catch (Exception e) {
            if (exceptionsOn) {
                ctx.logStackTrace(e);
                throw e;
            }
        }
        return this;
    }
    private String escape(String raw) {
        String escaped = raw;
        escaped = escaped.replace("\\", "\\\\");
        escaped = escaped.replace("\"", "\\\"");
        escaped = escaped.replace("\b", "\\b");
        escaped = escaped.replace("\f", "\\f");
        escaped = escaped.replace("\n", "\\n");
        escaped = escaped.replace("\r", "\\r");
        escaped = escaped.replace("\t", "\\t");
        // TODO: escape other non-printing characters using uXXXX notation
        return escaped;
    }

    /**
     * Executes a CLI command on the local router's management tree and returns the result as an Json Array.
     *
     * @param command CLI command
     * @return result
     * @throws Exception If an exception occurs and exceptionOn() is set (default)
     */
    public String executeWithResult(String command) throws Exception {
        String[] result;
        try {
            result = executor.executeWithResult(command);
            if (result == null) {
                result = new String[2];
                result[0] = TreeCommands.RESULT;
                if (command.startsWith("cc ")) {
                    if (executor.getContext().equals(""))
                        result[1] = "/";
                    else
                        result[1] = executor.getContext();
                } else
                    result[1] = "";
            }
        } catch (Exception e) {
            result = new String[2];
            result[0] = TreeCommands.ERROR;
            result[1] = e.getMessage();
            if (exceptionsOn) {
                ctx.logStackTrace(e);
                throw e;
            }
        }
        StringBuffer sb = new StringBuffer("[");
        for (int i = 0; i < result.length; i++) {
            if (i != 0)
                sb.append(",");
            sb.append('"');
            sb.append(escape(result[i]));
            sb.append('"');
        }
        if (command.equals("help")) {
            sb.append(",");
            sb.append('"');
            sb.append("Result:");
            sb.append('"');
            sb.append(",");
            sb.append('"');
            sb.append("Shell Commands:");
            sb.append('"');
            sb.append(",");
            sb.append('"');
            sb.append('"');
            sb.append(",");
            sb.append('"');
            sb.append("cc <context>                     Change to Context <context>");
            sb.append('"');
            sb.append(",");
            sb.append('"');
            sb.append("lc [<context>]                   List the Content of <context>");
            sb.append('"');
        }
        sb.append("]");
        return sb.toString();
    }
}
