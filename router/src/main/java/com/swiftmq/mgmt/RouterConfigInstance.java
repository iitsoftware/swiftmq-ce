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

import com.swiftmq.swiftlet.Swiftlet;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.util.SwiftUtilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class RouterConfigInstance extends EntityList {

    Entity currentEntity = null;

    /**
     * @SBGen Constructor
     */
    protected RouterConfigInstance() {
        super(null, null, null, null, null);
    }

    protected boolean isSetParent() {
        return false;
    }

    public int getDumpId() {
        return MgmtFactory.CONFIGINSTANCE;
    }

    public void writeContent(DataOutput out)
            throws IOException {
        super.writeContent(out);
    }

    public void readContent(DataInput in)
            throws IOException {
        super.readContent(in);
    }

    /**
     * @param commandRegistry
     * @SBGen Method set commandRegistry
     */
    public void setCommandRegistry(CommandRegistry commandRegistry) {
        // SBgen: Assign variable
        this.commandRegistry = commandRegistry;
    }

    /**
     * @return
     * @SBGen Method get commandRegistry
     */
    public CommandRegistry getCommandRegistry() {
        // SBgen: Get variable
        return (commandRegistry);
    }

    public synchronized void addEntity(Entity entity)
            throws EntityAddException {
        Entity config = entity;
        if (entityAddListener != null)
            config = entityAddListener.onConfigurationAdd(this, entity);
        //config.setParent(this) <--- das hier nicht, sonst wird im Explorer falsch adressiert (ctx inkl. routername)
        entities.put(config.getName(), config);
        notifyEntityWatchListeners(true, config);
    }

    /**
     * @return
     * @SBGen Method get configurations
     */
    public Map getConfigurations() {
        // SBgen: Get variable
        return (entities);
    }

    public void clearConfigurations() {
        entities.clear();
        properties.clear();
    }

    public Object getContext(Object currentContext, String[] context, int stacklevel) {
        if (context == null || context.length == 0)
            return null;
        if (currentContext == null) {
            Entity entity = (Entity) SwiftUtilities.getFirstStartsWith(entities, context[stacklevel]);
            if (entity == null)
                return null;
            if (stacklevel == context.length - 1)
                return entity;
            return getContext(entity, context, stacklevel + 1);
        } else {
            Entity entity = (Entity) SwiftUtilities.getFirstStartsWith((Map) ((Entity) currentContext).getEntities(), context[stacklevel]);
            if (entity == null)
                return null;
            if (stacklevel == context.length - 1)
                return entity;
            return getContext(entity, context, stacklevel + 1);
        }
    }

    private String stateToString(int state) {
        if (state == Swiftlet.STATE_ACTIVE)
            return "ACTIVE";
        if (state == Swiftlet.STATE_INACTIVE)
            return "INACTIVE";
        return "UNKNOWN";
    }

    private String[] dirSwiftlets() {
        ArrayList al = new ArrayList();
        al.add(TreeCommands.RESULT);
        al.add("Swiftlet Name       State    Description");
        al.add("-------------------------------------------------------------");
        for (Iterator iter = entities.keySet().iterator(); iter.hasNext(); ) {
            String name = (String) iter.next();
            Configuration c = (Configuration) entities.get(name);
            MetaData meta = c.getMetaData();
            StringBuffer b = new StringBuffer();
            b.append(SwiftUtilities.fillToLength(name, 20));
            try {
                b.append(SwiftUtilities.fillToLength(stateToString(SwiftletManager.getInstance().getSwiftletState(name)), 9));
            } catch (Exception ignored) {
            }
            if (meta != null)
                b.append(meta.getDescription());
            else
                b.append("<not specified>");
            al.add(b.toString());
        }
        return (String[]) al.toArray(new String[al.size()]);
    }

    private String[] dirEntityList(EntityList list) {
        ArrayList al = new ArrayList();
        al.add(TreeCommands.RESULT);
        al.add("Entity List: " + list.getDisplayName());
        al.add("Description: " + list.getDescription());
        al.add("");
        Map m = list.getEntities();
        if (m.size() == 0) {
            al.add("List contains no Entities.");
        } else {
            al.add("Number of Entities in this List: " + m.size());
            al.add("--------------------------------");
            for (Iterator iter = m.keySet().iterator(); iter.hasNext(); ) {
                String name = (String) iter.next();
                al.add(name);
            }
        }
        return (String[]) al.toArray(new String[al.size()]);
    }

    private String[] dirEntityList(EntityList list, Authenticator authenticator) {
        Map m = list.getEntities();
        List stripped = new ArrayList();
        for (Iterator iter = m.entrySet().iterator(); iter.hasNext(); ) {
            Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
            if (authenticator.isContextGranted(entity))
                stripped.add(entity.getName());
        }

        ArrayList al = new ArrayList();
        al.add(TreeCommands.RESULT);
        al.add("Entity List: " + list.getDisplayName());
        al.add("Description: " + list.getDescription());
        al.add("");
        if (stripped.size() == 0) {
            al.add("List contains no Entities.");
        } else {
            al.add("Number of Entities in this List: " + m.size());
            al.add("--------------------------------");
            al.addAll(stripped);
        }
        return (String[]) al.toArray(new String[al.size()]);
    }

    private String[] dirEntity(Entity entity) {
        ArrayList al = new ArrayList();
        al.add(TreeCommands.RESULT);
        al.add("Entity:      " + entity.getDisplayName());
        al.add("Description: " + entity.getDescription());
        al.add("");
        Map m = entity.getProperties();
        if (m.size() == 0) {
            al.add("Entity contains no Properties.");
        } else {
            al.add("Properties for this Entity:");
            al.add("");
            al.add("Name                                    Current Value");
            al.add("--------------------------------------------------------------");
            for (Iterator iter = m.keySet().iterator(); iter.hasNext(); ) {
                String name = (String) iter.next();
                Property p = (Property) entity.getProperty(name);
                if (p.isReadOnly())
                    name += " (R/O)";
                StringBuffer b = new StringBuffer();
                b.append(SwiftUtilities.fillToLength(name, 40));
                Object v = p.getValue();
                if (v == null)
                    b.append("<not set>");
                else
                    b.append(v.toString());
                al.add(b.toString());
            }
        }
        al.add("");
        m = entity.getEntities();
        if (m.size() == 0) {
            al.add("Entity contains no Sub-Entities.");
        } else {
            al.add("Sub-Entities of this Entity:");
            al.add("----------------------------");
            for (Iterator iter = m.keySet().iterator(); iter.hasNext(); ) {
                String name = (String) iter.next();
                al.add(name);
            }
        }
        return (String[]) al.toArray(new String[al.size()]);
    }

    private String[] dirEntity(Entity entity, Authenticator authenticator) {
        ArrayList al = new ArrayList();
        al.add(TreeCommands.RESULT);
        al.add("Entity:      " + entity.getDisplayName());
        al.add("Description: " + entity.getDescription());
        al.add("");
        Map m = entity.getProperties();
        if (m.size() == 0) {
            al.add("Entity contains no Properties.");
        } else {
            al.add("Properties for this Entity:");
            al.add("");
            al.add("Name                                    Current Value");
            al.add("--------------------------------------------------------------");
            for (Iterator iter = m.keySet().iterator(); iter.hasNext(); ) {
                String name = (String) iter.next();
                if (authenticator.isPropertyGranted(entity, name)) {
                    Property p = entity.getProperty(name);
                    if (p.isReadOnly() || authenticator.isPropertyReadOnly(entity, name))
                        name += " (R/O)";
                    StringBuffer b = new StringBuffer();
                    b.append(SwiftUtilities.fillToLength(name, 40));
                    Object v = p.getValue();
                    if (v == null)
                        b.append("<not set>");
                    else
                        b.append(v.toString());
                    al.add(b.toString());
                }
            }
        }
        al.add("");
        m = entity.getEntities();
        if (m.size() == 0) {
            al.add("Entity contains no Sub-Entities.");
        } else {
            List stripped = new ArrayList();
            for (Iterator iter = m.entrySet().iterator(); iter.hasNext(); ) {
                Entity e = (Entity) ((Map.Entry) iter.next()).getValue();
                if (authenticator.isContextGranted(e))
                    stripped.add(e.getName());
            }
            al.add("Sub-Entities of this Entity:");
            al.add("----------------------------");
            al.addAll(stripped);
        }
        return (String[]) al.toArray(new String[al.size()]);
    }

    private String[] dirContext(Object context, String[] cmd) {
        if (context == null || cmd.length == 2 && cmd[1].equals("/"))
            //return dirSwiftlets();
            return dirEntity(this);
        if (context instanceof EntityList)
            return dirEntityList((EntityList) context);
        else
            return dirEntity((Entity) context);
    }

    private String[] dirContext(String[] ctx, Object context, String[] cmd, Authenticator authenticator) {
        if (context == null || cmd.length == 2 && cmd[1].equals("/"))
            //return dirSwiftlets();
            return dirEntity(this, authenticator);
        if (context instanceof EntityList)
            return dirEntityList((EntityList) context, authenticator);
        else
            return dirEntity((Entity) context, authenticator);
    }

    private String[] getContextProp(Entity context, String[] cmd) {
        String[] rc = null;
        Property prop = context.getProperty(cmd[1]);
        if (prop != null) {
            String s = null;
            if (prop.getValue() != null) {
                s = prop.getValue().toString();
                rc = new String[]{prop.getName(), s.toString()};
            } else
                rc = new String[]{prop.getName()};
        }
        return rc;
    }

    private String[] getContextEntities(Entity context) {
        return context.getEntityNames();
    }

    public String[] executeInternalCommand(String[] context, String[] cmd) {
        String[] result = null;
        if (cmd[0].equals(TreeCommands.GET_CONTEXT_PROP)) {
            Entity ctx = (Entity) getContext(null, context, 0);
            if (ctx == null)
                result = null;
            else
                result = getContextProp(ctx, cmd);
        } else if (cmd[0].equals(TreeCommands.GET_CONTEXT_ENTITIES)) {
            Entity ctx = (Entity) getContext(null, context, 0);
            if (ctx == null)
                result = null;
            else
                result = getContextEntities(ctx);
        } else if (cmd[0].equals(TreeCommands.DIR_CONTEXT)) {
            result = dirContext(getContext(null, context, 0), cmd);
        } else if (cmd[0].equals(TreeCommands.CHANGE_CONTEXT)) {
            Object c = getContext(null, context, 0);
            if (cmd.length == 2) {
                String[] ctx = SwiftUtilities.tokenize(cmd[1], "/");
                c = getContext(c, ctx, 0);
                if (c == null)
                    result = new String[]{TreeCommands.ERROR, "Unknown context"};
            }
        }
        return result;
    }

    public String[] executeInternalCommand(String[] context, String[] cmd, Authenticator authenticator) {
        AuthenticatorHolder.threadLocal.set(authenticator);
        try {
            String[] result = null;
            if (cmd[0].equals(TreeCommands.GET_CONTEXT_PROP)) {
                Entity ctx = (Entity) getContext(null, context, 0);
                if (ctx == null)
                    result = null;
                else {
                    if (!authenticator.isContextGranted(ctx))
                        return new String[]{TreeCommands.ERROR, "Access to this context is not granted"};
                    result = getContextProp(ctx, cmd);
                }
            } else if (cmd[0].equals(TreeCommands.GET_CONTEXT_ENTITIES)) {
                Entity ctx = (Entity) getContext(null, context, 0);
                if (ctx == null)
                    result = null;
                else {
                    if (!authenticator.isContextGranted(ctx))
                        return new String[]{TreeCommands.ERROR, "Access to this context is not granted"};
                    result = getContextEntities(ctx);
                }
            } else if (cmd[0].equals(TreeCommands.DIR_CONTEXT)) {
                result = dirContext(context, getContext(null, context, 0), cmd, authenticator);
            } else if (cmd[0].equals(TreeCommands.CHANGE_CONTEXT)) {
                Object c = getContext(null, context, 0);
                if (cmd.length == 2) {
                    String[] ctx = SwiftUtilities.tokenize(cmd[1], "/");
                    c = getContext(c, ctx, 0);
                    if (c == null)
                        result = new String[]{TreeCommands.ERROR, "Unknown context"};
                    else {
                        if (!authenticator.isContextGranted((Entity) c))
                            return new String[]{TreeCommands.ERROR, "Access to this context is not granted"};
                    }
                }
            }
            return result;
        } finally {
            AuthenticatorHolder.threadLocal.set(null);
        }
    }

    public String[] executeCommand(String[] context, String[] cmd) {
        Object c = getContext(null, context, 0);
        if (c != null) {
            if (c instanceof Entity) {
                currentEntity = (Entity) c;
                commandRegistry.setDefaultCommand(new CommandExecutor() {
                    public String[] execute(String[] context, Entity entity, String[] cmd) {
                        return currentEntity.getCommandRegistry().executeCommand(context, cmd);
                    }
                });
            } else
                commandRegistry.setDefaultCommand(null);
        }
        return commandRegistry.executeCommand(context, cmd);
    }

    public String[] executeCommand(String[] context, String[] cmd, Authenticator authenticator) {
        AuthenticatorHolder.threadLocal.set(authenticator);
        try {
            return executeCommand(context, cmd);
        } finally {
            AuthenticatorHolder.threadLocal.set(null);
        }
    }

    @Override
    public String toJson() {
        StringBuffer s = new StringBuffer();
        s.append("{");
        s.append(quote("nodetype")).append(": ");
        s.append(quote("root")).append(", ");
        s.append(quote("name")).append(": ");
        s.append(quote(SwiftletManager.getInstance().getRouterName()));
        if (entities != null) {
            s.append(", ");
            s.append(quote("entities")).append(": ");
            s.append("[");
            boolean first = true;
            for (Object o : entities.entrySet()) {
                if (!first)
                    s.append(", ");
                first = false;
                Entity e = (Entity) ((Map.Entry) o).getValue();
                s.append("{");
                s.append(quote("nodetype")).append(": ");
                if (e instanceof EntityList)
                    s.append(quote("entitylist")).append(", ");
                else
                    s.append(quote("entity")).append(", ");
                s.append(quote("name")).append(": ");
                s.append(quote(e.getName())).append(", ");
                s.append(quote("displayName")).append(": ");
                s.append(quote(e.getDisplayName())).append(", ");
                s.append(quote("description")).append(": ");
                s.append(quote(e.getDescription()));
                s.append("}");

            }
            s.append("]");
        }
        if (commandRegistry != null && commandRegistry.getCommands() != null) {
            s.append(", ");
            s.append(quote("commands")).append(": ");
            s.append("[");
            List cmds = commandRegistry.getCommands();
            boolean first = true;
            for (int i = 0; i < cmds.size(); i++) {
                Command command = (Command) cmds.get(i);
                if (commandIncluded(command, new String[]{"help", "sum", "show template"})) {
                    if (!first) {
                        s.append(", ");
                    }
                    first = false;
                    s.append(((Command) cmds.get(i)).toJson());
                }
            }
            s.append("]");
        }
        s.append("}");
        return s.toString();

    }
}

