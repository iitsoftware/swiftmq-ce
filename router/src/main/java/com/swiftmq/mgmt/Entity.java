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

import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.DumpableFactory;
import com.swiftmq.tools.dump.Dumpalizer;

import javax.swing.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * A Entity represents a node within the management tree. It may contain Property objects,
 * as well as sub-entities. Each Entity must have a CommandRegistry where commands are
 * registered to be performed on that Entity or their childs.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class Entity implements Dumpable {
    public static final String SET_COMMAND = "set";
    String name = null;
    String displayName = null;
    String description = null;
    boolean dynamic = false;
    transient Object userObject = null;
    transient Object dynamicObject = null;
    String[] dynamicPropNames = null;
    CommandRegistry commandRegistry = null;
    Entity parent = null;
    Map properties = null;
    String state = null;
    byte[] imageArray = null;
    transient ImageIcon imageIcon = null;
    transient String iconFilename = null;
    Map entities = null;
    transient EntityAddListener entityAddListener;
    transient EntityRemoveListener entityRemoveListener;
    transient ArrayList watchListeners = null;
    protected DumpableFactory factory = new MgmtFactory();
    transient boolean upgrade = false;
    volatile String[] _ctx = null;
    volatile String[] _dctx = null;


    /**
     * Creates a new Entity.
     *
     * @param name        the name of the entity.
     * @param displayName the display name.
     * @param description a description.
     * @param state       the state (not used at the moment).
     */
    public Entity(String name, String displayName, String description, String state) {
        // SBgen: Assign variables
        this.name = name;
        this.displayName = displayName;
        this.description = description;
        this.state = state;
        // SBgen: End assign
        entities = new ClonableMap();
        properties = new ClonableMap();
    }

    protected Entity() {
        this(null, null, null, null);
    }

    public int getDumpId() {
        return MgmtFactory.ENTITY;
    }

    protected boolean isSetParent() {
        return true;
    }

    protected void writeDump(DataOutput out, String s) throws IOException {
        if (s == null)
            out.writeByte(0);
        else {
            out.writeByte(1);
            out.writeUTF(s);
        }
    }

    protected String readDump(DataInput in) throws IOException {
        byte set = in.readByte();
        if (set == 1)
            return in.readUTF();
        return null;
    }

    protected void writeDump(DataOutput out, String[] s) throws IOException {
        if (s == null)
            out.writeByte(0);
        else {
            out.writeByte(1);
            out.writeInt(s.length);
            for (int i = 0; i < s.length; i++)
                out.writeUTF(s[i]);
        }
    }

    protected String[] readDumpStringArray(DataInput in) throws IOException {
        byte set = in.readByte();
        if (set == 1) {
            String[] s = new String[in.readInt()];
            for (int i = 0; i < s.length; i++)
                s[i] = in.readUTF();
            return s;
        }
        return null;
    }

    protected void writeDump(DataOutput out, byte[] s) throws IOException {
        if (s == null)
            out.writeByte(0);
        else {
            out.writeByte(1);
            out.writeInt(s.length);
            out.write(s);
        }
    }

    protected byte[] readDumpByteArray(DataInput in) throws IOException {
        byte set = in.readByte();
        if (set == 1) {
            byte[] s = new byte[in.readInt()];
            in.readFully(s);
            return s;
        }
        return null;
    }

    protected void writeDump(DataOutput out, Dumpable d) throws IOException {
        if (d == null)
            out.writeByte(0);
        else {
            out.writeByte(1);
            Dumpalizer.dump(out, d);
        }
    }

    protected Dumpable readDumpDumpable(DataInput in, DumpableFactory factory) throws IOException {
        byte set = in.readByte();
        if (set == 1) {
            return Dumpalizer.construct(in, factory);
        }
        return null;
    }

    protected void writeDump(DataOutput out, Map map) throws IOException {
        if (map == null)
            out.writeByte(0);
        else {
            out.writeByte(1);
            out.writeInt(map.size());
            for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); ) {
                Dumpalizer.dump(out, (Dumpable) ((Map.Entry) iter.next()).getValue());
            }
        }
    }

    protected ClonableMap readDumpDumpablePropMap(DataInput in, DumpableFactory factory) throws IOException {
        byte set = in.readByte();
        if (set == 1) {
            ClonableMap map = new ClonableMap();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                Property prop = (Property) Dumpalizer.construct(in, factory);
                prop.setParent(this);
                map.put(prop.getName(), prop);
            }
            return map;
        }
        return null;
    }

    protected ClonableMap readDumpDumpableEntityMap(DataInput in, DumpableFactory factory) throws IOException {
        byte set = in.readByte();
        if (set == 1) {
            ClonableMap map = new ClonableMap();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                Entity entity = (Entity) Dumpalizer.construct(in, factory);
                if (isSetParent())
                    entity.setParent(this);
                map.put(entity.getName(), entity);
            }
            return map;
        }
        return null;
    }

    public synchronized void writeContent(DataOutput out)
            throws IOException {
        writeDump(out, name);
        writeDump(out, displayName);
        writeDump(out, description);
        writeDump(out, state);
        out.writeBoolean(dynamic);
        writeDump(out, dynamicPropNames);
        writeDump(out, commandRegistry);
        writeDump(out, imageArray);
        writeDump(out, properties);
        writeDump(out, entities);
    }

    public void readContent(DataInput in)
            throws IOException {
        name = readDump(in);
        displayName = readDump(in);
        description = readDump(in);
        state = readDump(in);
        dynamic = in.readBoolean();
        dynamicPropNames = readDumpStringArray(in);
        commandRegistry = (CommandRegistry) readDumpDumpable(in, factory);
        imageArray = readDumpByteArray(in);
        properties = readDumpDumpablePropMap(in, factory);
        entities = readDumpDumpableEntityMap(in, factory);
    }

    /**
     * Internal use only.
     */
    public void setImageArray(byte[] array) {
        this.imageArray = array;
    }

    void setIconFilename(String iconFilename) {
        this.iconFilename = iconFilename;
    }

    String getIconFilename() {
        return iconFilename;
    }

    /**
     * Internal use only.
     */
    public ImageIcon getIcon() {
        if (imageArray == null)
            return null;
        if (imageIcon == null)
            imageIcon = new ImageIcon(imageArray);
        return imageIcon;
    }

    /**
     * Internal use only.
     */
    public void setDynamic(boolean b) {
        this.dynamic = b;
    }

    /**
     * Internal use only.
     */
    public boolean isDynamic() {
        return dynamic;
    }

    /**
     * Attach a user object to this entity.
     *
     * @param userObject user object.
     */
    public void setUserObject(Object userObject) {
        this.userObject = userObject;
    }


    /**
     * Returns the user object.
     *
     * @return user object.
     */
    public Object getUserObject() {
        return userObject;
    }


    /**
     * Attach a dynamic object to this entity.
     * In case this entity is dynamic (part of the usage list), and
     * there is a dynamic object which corresponds to this entity,
     * e.g. a connection object, this should be attached with this method.
     *
     * @param dynamicObject dynamic object.
     */
    public void setDynamicObject(Object dynamicObject) {
        this.dynamicObject = dynamicObject;
    }


    /**
     * Returns the dynamic object.
     *
     * @return dynamic object.
     */
    public Object getDynamicObject() {
        return dynamicObject;
    }


    /**
     * Set an array of dynamic property names.
     * These are displayed in the dynamic chart of a dynamic entity,
     * each with a separate colored line. The type of these dynamic
     * properties must be of Integer, and, of course, the properties
     * must be added to this entity.
     *
     * @param dynamicPropNames array of property names.
     */
    public void setDynamicPropNames(String[] dynamicPropNames) {
        this.dynamicPropNames = dynamicPropNames;
    }


    /**
     * Returns the dynamic property names.
     *
     * @return array of property names.
     */
    public String[] getDynamicPropNames() {
        return dynamicPropNames;
    }

    /**
     * Internal use only.
     */
    public String[] getContext() {
        if (_ctx != null)
            return _ctx;
        ArrayList al = new ArrayList();
        Entity actEntity = this;
        while (actEntity != null) {
            al.add(actEntity.getName());
            actEntity = actEntity.getParent();
        }
        String[] ctx = new String[al.size()];
        int j = 0;
        for (int i = al.size() - 1; i >= 0; i--)
            ctx[j++] = (String) al.get(i);
        _ctx = ctx;
        return _ctx;
    }

    /**
     * Internal use only.
     */
    public String[] getDisplayContext() {
        if (_dctx != null)
            return _dctx;
        ArrayList al = new ArrayList();
        Entity actEntity = this;
        while (actEntity != null) {
            al.add(actEntity.getDisplayName());
            actEntity = actEntity.getParent();
        }
        String[] ctx = new String[al.size()];
        int j = 0;
        for (int i = al.size() - 1; i >= 0; i--)
            ctx[j++] = (String) al.get(i);
        _dctx = ctx;
        return _dctx;
    }


    /**
     * Creates the commands out of the command registry. Normally,
     * this is performed automatically, except for dynamic entities.
     *
     * @see EntityList
     */
    public void createCommands() {
        commandRegistry = new CommandRegistry("Context '" + name + "'", null);
        CommandExecutor setExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length < 2 || cmd.length > 3)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try '" + SET_COMMAND + " <prop> [<value>]'"};
                String[] result = null;
                Property p = getProperty(cmd[1]);
                if (p == null)
                    result = new String[]{TreeCommands.ERROR, "Unknown Property: " + cmd[1]};
                else if (p.isReadOnly())
                    result = new String[]{TreeCommands.ERROR, "Property is read-only."};
                else {
                    try {
                        if (cmd.length == 2)
                            p.setValue(null);
                        else
                            p.setValue(Property.convertToType(p.getType(), cmd[2]));
                        if (p.isRebootRequired())
                            result = new String[]{TreeCommands.INFO, "To activate this Property Change, a Reboot of this Router is required."};
                    } catch (Exception e) {
                        result = new String[]{TreeCommands.ERROR, e.getMessage()};
                    }
                }
                return result;
            }
        };
        Command setCommand = new Command(SET_COMMAND, SET_COMMAND + " <prop> [<value>]", "Set Property <prop> to Value <value> or null", true, setExecutor);
        commandRegistry.addCommand(setCommand);
        CommandExecutor describeExecutor = new CommandExecutor() {
            private String check(Object o) {
                return o == null ? "<not set>" : o.toString();
            }

            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length != 2)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'describe <prop>'"};
                String[] result = null;
                Property p = getProperty(cmd[1]);
                if (p == null)
                    result = new String[]{TreeCommands.ERROR, "Unknown Property: " + cmd[1]};
                else {
                    result = new String[13];
                    result[0] = TreeCommands.RESULT;
                    result[1] = "Property Name  : " + p.getName();
                    result[2] = "Display Name   : " + check(p.getDisplayName());
                    result[3] = "Description    : " + check(p.getDescription());
                    result[4] = "Type           : " + p.getType();
                    result[5] = "Min. Value     : " + check(p.getMinValue());
                    result[6] = "Max. Value     : " + check(p.getMaxValue());
                    result[7] = "Default Value  : " + check(p.getDefaultValue());
                    result[8] = "Poss. Values   : " + check(p.getPossibleValues());
                    result[9] = "Actual Value   : " + check(p.getValue());
                    result[10] = "Mandatory     : " + p.isMandatory();
                    result[11] = "Read Only      : " + p.isReadOnly();
                    result[12] = "Reboot Required: " + p.isRebootRequired();
                }
                return result;
            }
        };
        Command describeCommand = new Command("describe", "describe <prop>", "Show full Description of Property <prop>", true, describeExecutor);
        commandRegistry.addCommand(describeCommand);

        // Do it for all sub-entities
        for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
            Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
            entity.createCommands();
        }
    }


    /**
     * Returns the command registry.
     *
     * @return command registry.
     */
    public CommandRegistry getCommandRegistry() {
        return commandRegistry;
    }


    /**
     * Set the entity name.
     *
     * @param name name.
     */
    public void setName(String name) {
        // SBgen: Assign variable
        this.name = name;
    }


    /**
     * Returns the entity name.
     *
     * @return entity name.
     */
    public String getName() {
        // SBgen: Get variable
        return (name);
    }

    /**
     * Returns the display name.
     *
     * @return display name.
     */
    public String getDisplayName() {
        // SBgen: Get variable
        return (displayName);
    }


    /**
     * Returns the description.
     *
     * @return description.
     */
    public String getDescription() {
        // SBgen: Get variable
        return (description);
    }


    /**
     * Add a command to the command registry.
     *
     * @param name    command name.
     * @param command command.
     */
    public synchronized void addCommand(String name, Command command) {
        command.setParent(this);
        commandRegistry.addCommand(command);
    }


    /**
     * Remove a command from the command registry.
     *
     * @param name command name.
     */
    public synchronized void removeCommand(String name) {
        Command cmd = commandRegistry.findCommand(new String[]{name});
        if (cmd != null)
            commandRegistry.removeCommand(cmd);
    }

    /**
     * Internal use only.
     */
    public String getState() {
        // SBgen: Get variable
        return (state);
    }

    /**
     * Internal use only.
     */
    public void setState(String state) {
        // SBgen: Assign variable
        this.state = state;
    }


    /**
     * Add a property.
     *
     * @param name     property name.
     * @param property property.
     */
    public synchronized void addProperty(String name, Property property) {
        property.setParent(this);
        properties.put(name, property);
    }


    /**
     * Remove a property.
     *
     * @param name property name.
     */
    public synchronized void removeProperty(String name) {
        properties.remove(name);
    }


    /**
     * Returns a property.
     *
     * @param name property name.
     * @return property.
     */
    public synchronized Property getProperty(String name) {
        // SBgen: Get variable
        return (Property) properties.get(name);
    }


    /**
     * Returns a Map of all properties.
     *
     * @return map of properties.
     */
    public synchronized Map getProperties() {
        // SBgen: Get variable
        return ((ClonableMap) properties).createCopy();
    }


    /**
     * Add an Entity.
     *
     * @param entity entity.
     * @throws EntityAddException thrown by an EntityAddListener.
     */
    public synchronized void addEntity(Entity entity)
            throws EntityAddException {
        if (entityAddListener != null)
            entityAddListener.onEntityAdd(this, entity);
        entity.setParent(this);
        entities.put(entity.getName(), entity);
        notifyEntityWatchListeners(true, entity);
    }


    /**
     * Removes an Entity.
     *
     * @param entity entity.
     * @throws EntityRemoveException thrown by an EntityRemoveListener.
     */
    public synchronized void removeEntity(Entity entity)
            throws EntityRemoveException {
        if (entity == null)
            return;
        if (entityRemoveListener != null)
            entityRemoveListener.onEntityRemove(this, entity);
        entities.remove(entity.getName());
        entity.setParent(null);
        notifyEntityWatchListeners(false, entity);
    }


    /**
     * Removes all Entities.
     */
    public synchronized void removeEntities() {
        entities.clear();
    }


    /**
     * Removes an Entity with that dynamic object set.
     *
     * @param dynamicObject dynamic object.
     */
    public synchronized void removeDynamicEntity(Object dynamicObject) {
        for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
            Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
            if (entity.getDynamicObject() == dynamicObject) {
                entity.setDynamicObject(null);
                entity.setParent(null);
                iter.remove();
                notifyEntityWatchListeners(false, entity);
                break;
            }
        }
    }


    /**
     * Returns a Sub-Entity.
     *
     * @param name name.
     * @return Entity.
     */
    public synchronized Entity getEntity(String name) {
        // SBgen: Get variable
        return (Entity) entities.get(name);
    }


    /**
     * Returns an array with all sub-entity names
     *
     * @return array with all sub-entity names.
     */
    public synchronized String[] getEntityNames() {
        if (entities.size() == 0)
            return null;
        String[] rArr = new String[entities.size()];
        int i = 0;
        for (Iterator iter = entities.keySet().iterator(); iter.hasNext(); )
            rArr[i++] = (String) iter.next();
        return rArr;
    }


    /**
     * Returns a Map with all Entities.
     *
     * @return entity map.
     */
    public synchronized Map getEntities() {
        // SBgen: Get variable
        return ((ClonableMap) entities).createCopy();
    }

    protected void setParent(Entity parent) {
        // SBgen: Assign variable
        this.parent = parent;
    }


    /**
     * Returns the parent Entity.
     *
     * @return parent Entity.
     */
    public Entity getParent() {
        // SBgen: Get variable
        return (parent);
    }


    /**
     * Set the EntityAddListener.
     * There can only be 1 EntityAddListener which is responsible to verify the addition
     * and may be throw an EntityAddException.
     *
     * @param entityAddListener listener.
     */
    public void setEntityAddListener(EntityAddListener entityAddListener) {
        // SBgen: Assign variable
        this.entityAddListener = entityAddListener;
    }


    /**
     * Returns the EntityAddListener
     *
     * @return listener.
     */
    public EntityAddListener getEntityAddListener() {
        // SBgen: Get variable
        return (entityAddListener);
    }

    /**
     * Set the EntityRemoveListener.
     * There can only be 1 EntityRemoveListener which is responsible to verify the removal
     * and may be throw an EntityRemoveException.
     *
     * @param entityRemoveListener listener.
     */
    public void setEntityRemoveListener(EntityRemoveListener entityRemoveListener) {
        // SBgen: Assign variable
        this.entityRemoveListener = entityRemoveListener;
    }

    /**
     * Returns the EntityRemoveListener
     *
     * @return listener.
     */
    public EntityRemoveListener getEntityRemoveListener() {
        // SBgen: Get variable
        return (entityRemoveListener);
    }


    /**
     * Adds an EntityWatchListener.
     * There can be several of thos listeners registered at an Entity. They all are
     * informed on addition/removal of sub-entities after the action has been performed
     * (Entity added/removed).
     *
     * @param l listener.
     */
    public synchronized void addEntityWatchListener(EntityWatchListener l) {
        if (watchListeners == null)
            watchListeners = new ArrayList();
        watchListeners.add(l);
    }


    /**
     * Removes an EntityWatchListener.
     *
     * @param l listener.
     */
    public synchronized void removeEntityWatchListener(EntityWatchListener l) {
        if (watchListeners != null)
            watchListeners.remove(l);
    }

    protected void notifyEntityWatchListeners(boolean entityAdded, Entity entity) {
        if (watchListeners == null)
            return;
        for (int i = 0; i < watchListeners.size(); i++) {
            EntityWatchListener l = (EntityWatchListener) watchListeners.get(i);
            if (entityAdded)
                l.entityAdded(this, entity);
            else
                l.entityRemoved(this, entity);
        }
    }

    /**
     * Internal use only.
     */
    public Entity createCopy() {
        Entity entity = new Entity(name, displayName, description, state);
        entity.dynamic = dynamic;
        entity.dynamicPropNames = dynamicPropNames;
        entity.commandRegistry = commandRegistry;
        entity.properties = new ClonableMap();
        for (Iterator iter = properties.entrySet().iterator(); iter.hasNext(); ) {
            Property p = (Property) ((Map.Entry) iter.next()).getValue();
            Property copy = p.createCopy();
            copy.setParent(entity);
            entity.properties.put(copy.getName(), copy);
        }
        return entity;
    }

    public boolean isUpgrade() {
        return upgrade;
    }

    public void setUpgrade(boolean upgrade) {
        this.upgrade = upgrade;
    }

    public String toString() {
        StringBuffer s = new StringBuffer();
        s.append("\n[Entity, name=");
        s.append(name);
        s.append(", displayName=");
        s.append(displayName);
        s.append(", description=");
        s.append(description);
        s.append(", state=");
        s.append(state);
        s.append(", properties=");
        s.append(properties);
        s.append(", entities=");
        s.append(entities);
        s.append("]");
        return s.toString();
    }

    protected class ClonableMap extends TreeMap {
        public ClonableMap createCopy() {
            return (ClonableMap) clone();
        }
    }
}

