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

package com.swiftmq.impl.mgmt.standard.jmx;

import com.swiftmq.impl.mgmt.standard.SwiftletContext;
import com.swiftmq.mgmt.*;
import com.swiftmq.util.SwiftUtilities;

import javax.management.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class EntityMBean implements DynamicMBean, EntityWatchListener {
    SwiftletContext ctx = null;
    Entity entity = null;
    MBeanInfo info = null;
    ObjectName objectName = null;
    Map delegatedCmds = new HashMap();
    Map children = new HashMap();

    public EntityMBean(SwiftletContext ctx, Entity entity) {
        this.ctx = ctx;
        this.entity = entity;
        info = new MBeanInfo(getClass().getName(), entity.getDisplayName(),
                createAttributeInfo(),
                new MBeanConstructorInfo[]{},
                (entity instanceof EntityList) ? createEntityListOperationInfo() : createEntityOperationInfo(), new MBeanNotificationInfo[]{});
        ctx.jmxUtil.registerMBean(this);
        createChildren();
        entity.addEntityWatchListener(this);
    }

    private void createChildren() {
        Map entities = entity.getEntities();
        if (entities != null && entities.size() > 0) {
            for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
                Entity e = (Entity) ((Map.Entry) iter.next()).getValue();
                synchronized (children) {
                    children.put(e, new EntityMBean(ctx, e));
                }
            }
        }
    }

    private Command findCommand(CommandRegistry cr, String name) {
        if (cr == null)
            return null;
        ArrayList list = cr.getCommands();
        if (list == null)
            return null;
        for (int i = 0; i < list.size(); i++) {
            Command c = (Command) list.get(i);
            if (c.getName().equals(name))
                return c;
        }
        return null;
    }

    private MBeanAttributeInfo[] createAttributeInfo() {
        ArrayList list = new ArrayList();
        Map props = entity.getProperties();
        if (props != null && props.size() > 0) {
            for (Iterator iter = props.entrySet().iterator(); iter.hasNext(); ) {
                Property property = (Property) ((Map.Entry) iter.next()).getValue();
                list.add(new MBeanAttributeInfo(property.getName(), property.getType().getName(), property.getDisplayName(), true, !property.isReadOnly(), false));
            }
        }

        return (MBeanAttributeInfo[]) list.toArray(new MBeanAttributeInfo[list.size()]);
    }

    private MBeanOperationInfo[] createEntityOperationInfo() {
        ArrayList list = new ArrayList();
        CommandRegistry cmdReg = entity.getCommandRegistry();
        if (cmdReg != null) {
            ArrayList cmdList = cmdReg.getCommands();
            if (cmdList != null) {
                for (int i = 0; i < cmdList.size(); i++) {
                    Command cmd = (Command) cmdList.get(i);
                    if (cmd.isEnabled() && cmd.isGuiEnabled() && !cmd.isGuiForChild() && !cmd.getName().equals("new") && !cmd.getName().equals("view")) {
                        list.add(new MBeanOperationInfo(cmd.getName(), cmd.getDescription(), null, "java.lang.String[]", MBeanOperationInfo.ACTION));
                    }
                }
            }
        }
        if (entity.getParent() != null && entity.getParent() instanceof EntityList) {
            CommandRegistry cr = entity.getParent().getCommandRegistry();
            if (cr != null) {
                ArrayList al = cr.getCommands();
                if (al != null) {
                    for (int i = 0; i < al.size(); i++) {
                        Command c = (Command) al.get(i);
                        if (c.isEnabled() && (c.isGuiForChild() && !c.getName().equals("new")) || c.getName().equals("remove")) {
                            delegatedCmds.put(c.getName(), c);
                            if (c.getName().equals("view")) {
                                MBeanParameterInfo[] parInfos = new MBeanParameterInfo[2];
                                parInfos[0] = new MBeanParameterInfo("startindex", "java.lang.Integer", "Start Index");
                                parInfos[1] = new MBeanParameterInfo("stopindex", "java.lang.Integer", "Stop Index");
                                list.add(new MBeanOperationInfo(c.getName(), c.getDescription(), parInfos, "java.lang.String[]", MBeanOperationInfo.INFO));
                            } else if (c.getName().equals("remove")) {
                                MBeanParameterInfo[] parInfos = new MBeanParameterInfo[1];
                                parInfos[0] = new MBeanParameterInfo("messagekey", "java.lang.Integer", "Message Key");
                                list.add(new MBeanOperationInfo(c.getName(), c.getDescription(), parInfos, "java.lang.String[]", MBeanOperationInfo.ACTION));
                                list.add(new MBeanOperationInfo("purge", "Purge Queue", null, "java.lang.String[]", MBeanOperationInfo.ACTION));
                            } else
                                list.add(new MBeanOperationInfo(c.getName(), c.getDescription(), null, "java.lang.String[]", MBeanOperationInfo.ACTION));
                        }
                    }
                }
            }
        }
        return (MBeanOperationInfo[]) list.toArray(new MBeanOperationInfo[list.size()]);
    }

    private MBeanOperationInfo[] createEntityListOperationInfo() {
        MBeanOperationInfo[] infos = null;
        Command c = findCommand(entity.getCommandRegistry(), "new");
        if (c != null) {
            MBeanParameterInfo[] parInfos = null;
            Map props = ((EntityList) entity).getTemplate().getProperties();
            if (props != null) {
                parInfos = new MBeanParameterInfo[props.size() + 1];
                parInfos[0] = new MBeanParameterInfo("name", "java.lang.String", "Name of this new Entity");
                int i = 1;
                for (Iterator iter = props.entrySet().iterator(); iter.hasNext(); ) {
                    Property prop = (Property) ((Map.Entry) iter.next()).getValue();
                    parInfos[i++] = new MBeanParameterInfo(prop.getName(), prop.getType().getName(), prop.getDescription());
                }
            } else {
                parInfos = new MBeanParameterInfo[1];
                parInfos[0] = new MBeanParameterInfo("name", "java.lang.String", "Name of this new Entity");
            }
            infos = new MBeanOperationInfo[1];
            infos[0] = new MBeanOperationInfo(c.getName(), c.getDescription(), parInfos, "java.lang.String[]", MBeanOperationInfo.ACTION);
        } else
            infos = new MBeanOperationInfo[0];
        MBeanOperationInfo[] info2 = createEntityOperationInfo();
        if (info2 != null) {
            MBeanOperationInfo[] info3 = new MBeanOperationInfo[infos.length + info2.length];
            System.arraycopy(infos, 0, info3, 0, infos.length);
            System.arraycopy(info2, 0, info3, infos.length, info2.length);
            infos = info3;
        }
        return infos;
    }

    public void setObjectName(ObjectName objectName) {
        this.objectName = objectName;
    }

    public ObjectName getObjectName() {
        return objectName;
    }

    public Entity getEntity() {
        return entity;
    }

    public MBeanInfo getMBeanInfo() {
        return info;
    }

    public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
        Property property = entity.getProperty(attribute);
        if (property == null)
            throw new AttributeNotFoundException("Property '" + attribute + "' not found");
        return property.getValue();
    }

    public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        Property property = entity.getProperty(attribute.getName());
        if (property == null)
            throw new AttributeNotFoundException("Property '" + attribute.getName() + "' not found");
        try {
            property.setValue(attribute.getValue());
        } catch (Exception e) {
            throw new InvalidAttributeValueException(e.getMessage());
        }
    }

    public AttributeList getAttributes(String[] attributes) {
        AttributeList list = new AttributeList();
        for (int i = 0; i < attributes.length; i++) {
            Property property = entity.getProperty(attributes[i]);
            if (property != null)
                list.add(new Attribute(property.getName(), property.getValue()));
        }
        return list;
    }

    public AttributeList setAttributes(AttributeList attributes) {
        AttributeList list = new AttributeList();
        for (int i = 0; i < attributes.size(); i++) {
            Attribute a = (Attribute) attributes.get(i);
            Property property = entity.getProperty(a.getName());
            if (property != null) {
                list.add(a);
                try {
                    property.setValue(a.getValue());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return list;
    }

    public Object invoke(String actionName, Object params[], String signature[]) throws MBeanException, ReflectionException {
        if (actionName.equals("remove")) {
            String[] cmd = new String[3];
            cmd[0] = "remove";
            cmd[1] = entity.getName();
            cmd[2] = "0";
            if (params[0] != null)
                cmd[2] = params[0].toString();
            String[] res = entity.getParent().getCommandRegistry().executeCommand(null, cmd);
            if (res == null)
                return new String[]{"Operation successful."};
            if (res[0].equals(TreeCommands.ERROR))
                throw new MBeanException(new Exception(SwiftUtilities.concat(SwiftUtilities.cutFirst(res), " ")));
            return SwiftUtilities.cutFirst(res);
        } else if (actionName.equals("purge")) {
            String[] cmd = new String[3];
            cmd[0] = "remove";
            cmd[1] = entity.getName();
            cmd[2] = "*";
            String[] res = entity.getParent().getCommandRegistry().executeCommand(null, cmd);
            if (res == null)
                return new String[]{"Operation successful."};
            if (res[0].equals(TreeCommands.ERROR))
                throw new MBeanException(new Exception(SwiftUtilities.concat(SwiftUtilities.cutFirst(res), " ")));
            return SwiftUtilities.cutFirst(res);
        } else if (actionName.equals("view")) {
            String[] cmd = new String[4];
            cmd[0] = "view";
            cmd[1] = entity.getName();
            cmd[2] = "0";
            cmd[3] = "*";
            if (params[0] != null)
                cmd[2] = params[0].toString();
            if (params[1] != null)
                cmd[3] = params[1].toString();
            String[] res = entity.getParent().getCommandRegistry().executeCommand(null, cmd);
            if (res == null)
                return new String[]{"Queue is empty."};
            if (res[0].equals(TreeCommands.ERROR))
                throw new MBeanException(new Exception(SwiftUtilities.concat(SwiftUtilities.cutFirst(res), " ")));
            return SwiftUtilities.cutFirst(res);
        } else if (actionName.equals("new")) {
            try {
                Entity newEntity = ((EntityList) entity).createEntity();
                newEntity.setName((String) params[0]);
                newEntity.createCommands();
                Map props = newEntity.getProperties();
                if (props != null) {
                    int i = 1;
                    for (Iterator iter = props.entrySet().iterator(); iter.hasNext(); ) {
                        Property prop = (Property) ((Map.Entry) iter.next()).getValue();
                        Object value = params[i] == null ? prop.getDefaultValue() : params[i];
                        if (value != null)
                            prop.setValue(Property.convertToType(prop.getType(), value.toString()));
                        i++;
                    }
                }
                entity.addEntity(newEntity);
                return new String[]{"Entity created."};
            } catch (Exception e) {
                e.printStackTrace();
                throw new MBeanException(new Exception(e.toString()));
            }
        } else {
            String[] res = null;
            if (delegatedCmds.get(actionName) != null)
                res = entity.getParent().getCommandRegistry().executeCommand(null, new String[]{actionName, entity.getName()});
            else
                res = entity.getCommandRegistry().executeCommand(null, new String[]{actionName});
            if (res == null)
                return new String[]{"Operation successful."};
            ;
            if (res[0].equals(TreeCommands.ERROR))
                throw new MBeanException(new Exception(SwiftUtilities.concat(SwiftUtilities.cutFirst(res), " ")));
            return SwiftUtilities.cutFirst(res);
        }
    }

    public void entityAdded(Entity parent, Entity newEntity) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/entityAdded, newEntity=" + newEntity.getName() + " ...");
        synchronized (children) {
            children.put(newEntity, new EntityMBean(ctx, newEntity));
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/entityAdded, newEntity=" + newEntity.getName() + " done");
    }

    public void entityRemoved(Entity parent, Entity oldEntity) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/entityRemoved, oldEntity=" + oldEntity.getName() + " ...");
        synchronized (children) {
            EntityMBean mbean = (EntityMBean) children.remove(oldEntity);
            if (mbean != null)
                mbean.close();
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/entityRemoved, oldEntity=" + oldEntity.getName() + " done");
    }

    public void close() {
        ctx.jmxUtil.unregisterMBean(this);
        entity.removeEntityWatchListener(this);
        synchronized (children) {
            for (Iterator iter = children.entrySet().iterator(); iter.hasNext(); ) {
                ((EntityMBean) ((Map.Entry) iter.next()).getValue()).close();
            }
            children.clear();
        }
    }

    public String toString() {
        return "EntityMBean, name=" + entity.getName();
    }
}
