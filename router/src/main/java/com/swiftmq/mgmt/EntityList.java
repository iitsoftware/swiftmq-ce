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

import com.swiftmq.tools.util.ObjectCloner;
import com.swiftmq.util.SwiftUtilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.*;

/**
 * A EntityList object that extends Entity. The difference is that the EntityList
 * supports the dynamic addition/removal of sub-entities. It also owns an Entity
 * template to creates predefined sub-entities.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class EntityList extends Entity {
    static final DecimalFormat formatter = new DecimalFormat("###,##0.00", new DecimalFormatSymbols(Locale.US));
    public static final String NEW_COMMAND = "new";
    public static final String DEL_COMMAND = "delete";
    Entity template;
    Command newCommand = null;
    Command delCommand = null;
    boolean rebootOnNew = false;
    boolean rebootOnDel = false;
    boolean autoCreateNewDel = false;

    /**
     * Creates a new EntityList.
     *
     * @param name        name.
     * @param displayName display name.
     * @param description description.
     * @param state       not used yet.
     * @param template    the template for new sub-entities.
     * @param rebootOnNew states whether an addition is only active after reboot.
     * @param rebootOnDel states whether a removal is only active after reboot.
     */
    public EntityList(String name, String displayName, String description, String state, Entity template,
                      boolean rebootOnNew, boolean rebootOnDel) {
        super(name, displayName, description, state);
        this.template = template;
        this.rebootOnNew = rebootOnNew;
        this.rebootOnDel = rebootOnDel;
    }

    /**
     * Creates a new EntityList.
     *
     * @param name        name.
     * @param displayName display name.
     * @param description description.
     * @param state       not used yet.
     * @param template    the template for new sub-entities.
     */
    public EntityList(String name, String displayName, String description, String state, Entity template) {
        this(name, displayName, description, state, template, false, false);
    }

    /**
     * Creates a new EntityList.
     *
     * @param name             name.
     * @param displayName      display name.
     * @param description      description.
     * @param state            not used yet.
     * @param template         the template for new sub-entities.
     * @param autoCreateNewDel automatically create new/delete commands.
     * @return description.
     */
    public EntityList(String name, String displayName, String description, String state, Entity template, boolean autoCreateNewDel) {
        this(name, displayName, description, state, template, false, false);
        this.autoCreateNewDel = autoCreateNewDel;
    }

    protected EntityList() {
    }

    public int getDumpId() {
        return MgmtFactory.ENTITYLIST;
    }

    public synchronized void writeContent(DataOutput out)
            throws IOException {
        super.writeContent(out);
        writeDump(out, template);
        writeDump(out, newCommand);
        writeDump(out, delCommand);
        out.writeBoolean(rebootOnNew);
        out.writeBoolean(rebootOnDel);
        out.writeBoolean(autoCreateNewDel);
    }

    public void readContent(DataInput in)
            throws IOException {
        super.readContent(in);
        template = (Entity) readDumpDumpable(in, factory);
        newCommand = (Command) readDumpDumpable(in, factory);
        delCommand = (Command) readDumpDumpable(in, factory);
        rebootOnNew = in.readBoolean();
        rebootOnDel = in.readBoolean();
        autoCreateNewDel = in.readBoolean();
    }

    /**
     * Creates a new Entity from the template.
     *
     * @return new entity.
     */
    public Entity createEntity() {
        Entity entity = null;
        try {
            entity = (Entity) ObjectCloner.copy(template, factory);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return entity;
    }

    private String check(Object o) {
        return o == null ? "<not set>" : o.toString();
    }

    private String[] dirTemplate(Entity entity) {
        ArrayList al = new ArrayList();
        al.add(TreeCommands.RESULT);
        al.add("Template:    " + entity.getDisplayName());
        al.add("Description: " + entity.getDescription());
        al.add("");
        Map m = entity.getProperties();
        if (m.size() == 0) {
            al.add("Template contains no Properties.");
        } else {
            al.add("Properties for this Template:");
            for (Iterator iter = m.keySet().iterator(); iter.hasNext(); ) {
                al.add("");
                String name = (String) iter.next();
                Property p = (Property) entity.getProperty(name);
                al.add("Property Name  : " + p.getName());
                al.add("Display Name   : " + check(p.getDisplayName()));
                al.add("Description    : " + check(p.getDescription()));
                al.add("Type           : " + p.getType());
                al.add("Min. Value     : " + check(p.getMinValue()));
                al.add("Max. Value     : " + check(p.getMaxValue()));
                al.add("Default Value  : " + check(p.getDefaultValue()));
                al.add("Poss. Values   : " + check(p.getPossibleValues()));
                al.add("Mandatory     : " + p.isMandatory());
                al.add("Read Only      : " + p.isReadOnly());
                al.add("Reboot Required: " + p.isRebootRequired());
            }
        }
        al.add("");
        m = entity.getEntities();
        if (m.size() == 0) {
            al.add("Template contains no Sub-Entities.");
        } else {
            al.add("Sub-Entities of this Template:");
            al.add("------------------------------");
            for (Iterator iter = m.keySet().iterator(); iter.hasNext(); ) {
                String name = (String) iter.next();
                al.add(name);
            }
        }
        return (String[]) al.toArray(new String[al.size()]);
    }

    private void sumProp(Map sumMap, Property p) {
        if (p.getValue() == null)
            return;
        if (p.getType() == Integer.class) {
            Integer val = (Integer) sumMap.get(p.getName());
            if (val == null)
                val = (Integer) p.getValue();
            else
                val = new Integer(val.intValue() + ((Integer) p.getValue()).intValue());
            sumMap.put(p.getName(), val);
        } else if (p.getType() == Long.class) {
            Long val = (Long) sumMap.get(p.getName());
            if (val == null)
                val = (Long) p.getValue();
            else
                val = new Long(val.longValue() + ((Long) p.getValue()).longValue());
            sumMap.put(p.getName(), val);
        }
        if (p.getType() == Double.class) {
            Double val = (Double) sumMap.get(p.getName());
            if (val == null)
                val = (Double) p.getValue();
            else
                val = new Double(val.doubleValue() + ((Double) p.getValue()).doubleValue());
            sumMap.put(p.getName(), val);
        } else if (p.getType() == String.class) {
            double pVal = Double.parseDouble((String) p.getValue());
            Double val = (Double) sumMap.get(p.getName());
            if (val == null)
                val = new Double(pVal);
            else
                val = new Double(val.doubleValue() + pVal);
            sumMap.put(p.getName(), val);
        }
    }

    private String[] sumProps(String[] propNames) {
        Entity tpl = getTemplate();
        Map m = tpl.getProperties();
        if (m.size() == 0) {
            return new String[]{TreeCommands.ERROR, "This entity list doesn't contain any property"};
        } else {
            for (int i = 0; i < propNames.length; i++) {
                Property p = (Property) m.get(propNames[i]);
                if (p == null)
                    return new String[]{TreeCommands.ERROR, "Property '" + propNames[i] + "' not found"};
                if (p.getType() != Integer.class && p.getType() != Double.class && p.getType() != Long.class && p.getType() != String.class)
                    return new String[]{TreeCommands.ERROR, "Property '" + propNames[i] + "' has wrong type"};
            }
        }
        ArrayList al = new ArrayList();
        al.add(TreeCommands.RESULT);
        m = getEntities();
        if (m.size() == 0) {
            al.add("Entity list is empty.");
        } else {
            Map sumMap = new HashMap();
            for (Iterator iter = m.entrySet().iterator(); iter.hasNext(); ) {
                Entity e = (Entity) ((Map.Entry) iter.next()).getValue();
                for (int i = 0; i < propNames.length; i++) {
                    sumProp(sumMap, e.getProperty(propNames[i]));
                }
            }
            al.add("count: " + m.size());
            for (int i = 0; i < propNames.length; i++) {
                Object val = sumMap.get(propNames[i]);
                if (val != null)
                    al.add(propNames[i] + ": " + (val instanceof Double ? formatter.format((Double) val) : val));
            }

        }
        return (String[]) al.toArray(new String[al.size()]);
    }

    public void createCommands() {
        commandRegistry = new CommandRegistry("Context '" + name + "'", null);
        CommandExecutor tplExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length != 2)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'show template'"};
                String[] result = dirTemplate(getTemplate());
                return result;
            }
        };
        Command tplCommand = new Command("show template", "show template", "Show the Template for new Entities", true, tplExecutor);
        commandRegistry.addCommand(tplCommand);

        CommandExecutor sumExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                String[] result = cmd.length > 1 ? sumProps(SwiftUtilities.cutFirst(cmd)) : new String[]{TreeCommands.RESULT, "count: " + getEntities().size()};
                ;
                return result;
            }
        };
        Command sumCommand = new Command("sum", "sum [<prop1> <prop2> ...]", "Computes the sum of property values", true, sumExecutor);
        commandRegistry.addCommand(sumCommand);

        CommandExecutor newExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length < 2 || cmd.length % 2 != 0)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try '" + NEW_COMMAND + " <name> [<prop> <value> ...]'"};
                String[] result = null;
                if (getEntity(cmd[1]) != null)
                    return new String[]{TreeCommands.ERROR, "Entity '" + cmd[1] + "' is already defined."};

                try {
                    Entity newEntity = (Entity) ObjectCloner.copy(template, new MgmtFactory());
                    newEntity.setName(cmd[1]);
                    newEntity.createCommands();

                    // fill props
                    for (int i = 2; i < cmd.length; i += 2) {
                        Property p = newEntity.getProperty(cmd[i]);
                        if (p == null)
                            return new String[]{TreeCommands.ERROR, "Unknown Property: " + cmd[i]};
                        try {
                            p.setValue(Property.convertToType(p.getType(), cmd[i + 1]));
                        } catch (Exception e) {
                            return new String[]{TreeCommands.ERROR, "Property '" + cmd[i] + "' Value '" + cmd[i + 1] + "': " + e.getMessage()};
                        }
                    }

                    // check if all mandatory props are set
                    Map m = newEntity.getProperties();
                    for (Iterator iter = m.entrySet().iterator(); iter.hasNext(); ) {
                        Property p = (Property) ((Map.Entry) iter.next()).getValue();
                        if (p.isMandatory() && p.getValue() == null)
                            return new String[]{TreeCommands.ERROR, "Mandatory Property '" + p.getName() + "' must be set."};
                    }
                    addEntity(newEntity);
                    if (rebootOnNew)
                        result = new String[]{TreeCommands.INFO, "To activate this Change, a Reboot of this Router is required."};
                } catch (Exception e) {
                    result = new String[]{TreeCommands.ERROR, e.getMessage()};
                }
                return result;
            }
        };
        String s = "Create a new Entity";
        newCommand = new Command(NEW_COMMAND, NEW_COMMAND + " <name> [<prop> <value> ...]", s, true, newExecutor, true, false);
        if ((rebootOnNew || autoCreateNewDel) && !isDynamic())
            commandRegistry.addCommand(newCommand);
        CommandExecutor delExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length != 2)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try '" + DEL_COMMAND + " <entity>'"};
                String[] result = null;
                Entity e = getEntity(cmd[1]);
                if (e == null)
                    return new String[]{TreeCommands.ERROR, "Unknown Entity: " + cmd[1]};
                try {
                    removeEntity(e);
                    if (rebootOnDel)
                        result = new String[]{TreeCommands.INFO, "To activate this Change, a Reboot of this Router is required."};
                } catch (Exception ex) {
                    result = new String[]{TreeCommands.ERROR, ex.getMessage()};
                }
                return result;
            }
        };
        delCommand = new Command(DEL_COMMAND, DEL_COMMAND + " <entity>", "Delete Entity", true, delExecutor, true, true);
        if ((rebootOnNew || autoCreateNewDel) && !isDynamic())
            commandRegistry.addCommand(delCommand);

        // Do it for all sub-entities
        for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
            Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
            entity.createCommands();
        }
    }

    /**
     * Returns the template for new sub-entities.
     *
     * @return template.
     */
    public Entity getTemplate() {
        // SBgen: Get variable
        return (template);
    }

    public void setEntityAddListener(EntityAddListener entityAddListener) {
        if (entityAddListener == null) {
            commandRegistry.removeCommand(newCommand);
        } else {
            commandRegistry.addCommand(newCommand);
        }
        super.setEntityAddListener(entityAddListener);
    }

    public void setEntityRemoveListener(EntityRemoveListener entityRemoveListener) {
        if (entityRemoveListener == null) {
            commandRegistry.removeCommand(delCommand);
        } else {
            commandRegistry.addCommand(delCommand);
        }
        super.setEntityRemoveListener(entityRemoveListener);
    }

    public String toJson() {
        StringBuffer s = new StringBuffer();
        s.append("{");
        s.append(quote("nodetype")).append(": ");
        s.append(quote("entitylist")).append(", ");
        s.append(quote("name")).append(": ");
        s.append(quote(name)).append(", ");
        s.append(quote("displayName")).append(": ");
        s.append(quote(displayName)).append(", ");
        s.append(quote("description")).append(": ");
        s.append(quote(description)).append(", ");
        s.append(quote("template")).append(": ");
        s.append(template.toJson());
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

    public String toString() {
        StringBuffer s = new StringBuffer();
        s.append("\n[EntityList, entity=");
        s.append(super.toString());
        s.append(", template=");
        s.append(template);
        s.append("]");
        return s.toString();
    }
}

