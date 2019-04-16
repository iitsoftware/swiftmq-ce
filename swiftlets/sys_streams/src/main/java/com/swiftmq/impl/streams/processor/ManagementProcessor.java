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

package com.swiftmq.impl.streams.processor;

import com.swiftmq.impl.streams.StreamContext;
import com.swiftmq.impl.streams.comp.io.ManagementInput;
import com.swiftmq.impl.streams.comp.message.Message;
import com.swiftmq.impl.streams.processor.po.POMgmtMessage;
import com.swiftmq.mgmt.*;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.util.SwiftUtilities;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ManagementProcessor implements EntityWatchListener, PropertyWatchListener {
    StreamContext ctx;
    ManagementInput input;
    EntityList entityList;
    Entity entity;
    AllPropertyWatchListener entityPropWatchListener;
    Property property;
    Object prevValue = null;
    Map<String, AllPropertyWatchListener> watchListenerMap = new ConcurrentHashMap<String, AllPropertyWatchListener>();
    MessageSelector messageSelector;

    public ManagementProcessor(StreamContext ctx, ManagementInput input) {
        this.ctx = ctx;
        this.input = input;
    }

    public ManagementInput getInput() {
        return input;
    }

    public void register() throws Exception {
        if (input.getSelector() != null) {
            messageSelector = new MessageSelector(input.getSelector());
            messageSelector.compile();
        }
        String[] name = SwiftUtilities.tokenize(input.context(), "/");
        String[] context = SwiftUtilities.cutLast(name);
        Object object = RouterConfiguration.Singleton().getContext(null, context, 0);
        if (object == null)
            throw new NullPointerException("CLI context not found: " + input.context());
        Entity e = (Entity) object;
        Entity child = e.getEntity(name[name.length - 1]);
        if (child != null && child instanceof EntityList) {
            entityList = (EntityList) child;
            Map entities = entityList.getEntities();
            if (entities != null) {
                for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
                    entityAdded(entityList, (Entity) ((Map.Entry) iter.next()).getValue());
                }
            }
            entityList.addEntityWatchListener(this);
            ctx.ctx.mgmtSwiftlet.fireEvent(true);
        } else {
            property = e.getProperty(name[name.length - 1]);
            if (property != null) {
                propertyValueChanged(property);
                property.addPropertyWatchListener(this);
                ctx.ctx.mgmtSwiftlet.fireEvent(true);
            } else if (child != null) {
                entity = child;
                entityPropWatchListener = new AllPropertyWatchListener(entity).register();
                ctx.ctx.mgmtSwiftlet.fireEvent(true);
            } else
                throw new NullPointerException("Nothing found at CLI context: " + input.context());
        }
    }

    public void deregister() {
        if (entityList != null) {
            entityList.removeEntityWatchListener(this);
            for (Iterator<Map.Entry<String, AllPropertyWatchListener>> iter = watchListenerMap.entrySet().iterator(); iter.hasNext(); ) {
                iter.next().getValue().deregister();
            }
            ctx.ctx.mgmtSwiftlet.fireEvent(false);
        }
        if (property != null) {
            property.removePropertyWatchListener(this);
            ctx.ctx.mgmtSwiftlet.fireEvent(false);
        }
        if (entityPropWatchListener != null) {
            entityPropWatchListener.deregister();
            ctx.ctx.mgmtSwiftlet.fireEvent(false);
        }
    }

    private Message addProperties(Message message, Entity entity) {
        message.property("name").set(entity.getName());
        message.property(ManagementInput.PROP_CTX).set(input.context());
        Map props = entity.getProperties();
        if (props != null) {
            for (Iterator iter = props.entrySet().iterator(); iter.hasNext(); ) {
                Property p = (Property) ((Map.Entry) iter.next()).getValue();
                message.property(p.getName().replace("-", "_")).set(p.getValue());
            }
        }
        return message;
    }

    private void sendMessage(Message message) {
        if (messageSelector == null || messageSelector.isSelected(message.getImpl()))
            ctx.streamProcessor.dispatch(new POMgmtMessage(null, this, message));
    }

    @Override
    public void entityAdded(Entity parent, Entity newEntity) {
        if (input.hasChangeCallback())
            watchListenerMap.put(newEntity.getName(), new AllPropertyWatchListener(newEntity).register());
        if (!input.hasAddCallback())
            return;
        Message message = addProperties(ctx.messageBuilder.message()
                .property(ManagementInput.PROP_OPER).set(ManagementInput.VAL_ADD)
                .property(ManagementInput.PROP_TIME).set(System.currentTimeMillis()), newEntity);
        sendMessage(message);
    }

    @Override
    public void entityRemoved(Entity parent, Entity delEntity) {
        if (input.hasChangeCallback()) {
            AllPropertyWatchListener watchListener = watchListenerMap.remove(delEntity.getName());
            if (watchListener != null)
                watchListener.deregister();
        }
        if (!input.hasRemoveCallback())
            return;
        Message message = addProperties(ctx.messageBuilder.message()
                .property(ManagementInput.PROP_OPER).set(ManagementInput.VAL_REMOVE)
                .property(ManagementInput.PROP_TIME).set(System.currentTimeMillis()), delEntity);
        sendMessage(message);
    }

    @Override
    public void propertyValueChanged(Property property) {
        if (!input.hasChangeCallback())
            return;
        if (property.getValue() != null && prevValue != null &&
                !property.getValue().equals(prevValue) || prevValue == null) {
            Message message = ctx.messageBuilder.message()
                    .property(ManagementInput.PROP_OPER).set(ManagementInput.VAL_CHANGE)
                    .property(ManagementInput.PROP_CTX).set(input.context())
                    .property(ManagementInput.PROP_TIME).set(System.currentTimeMillis())
                    .property("name").set(property.getParent().getName())
                    .property(property.getName().replace("-", "_")).set(property.getValue());
            List<String> propIncludes = input.getPropIncludes();
            if (propIncludes != null) {
                for (int i = 0; i < propIncludes.size(); i++) {
                    String propName = propIncludes.get(i);
                    String propNameU = propName.replace('-', '_');
                    if (message.property(propNameU).value().toObject() == null && property.getParent().getProperty(propName) != null) {
                        message.property(propNameU).set(property.getParent().getProperty(propName).getValue());
                    }
                }
            }
            sendMessage(message);
            prevValue = property.getValue();
        }
    }

    private class AllPropertyWatchListener implements PropertyWatchListener {
        Entity entity;
        Map<String, Object> prevValues = new ConcurrentHashMap<String, Object>();

        public AllPropertyWatchListener(Entity entity) {
            this.entity = entity;
        }

        @Override
        public void propertyValueChanged(Property property) {
            if (!input.hasChangeCallback())
                return;
            Object prevValue = prevValues.get(property.getName());
            if (property.getValue() != null && prevValue != null &&
                    !property.getValue().equals(prevValue) || prevValue == null) {
                Message message = ctx.messageBuilder.message()
                        .property(ManagementInput.PROP_OPER).set(ManagementInput.VAL_CHANGE)
                        .property(ManagementInput.PROP_CTX).set(input.context())
                        .property("name").set(entity.getName())
                        .property(ManagementInput.PROP_TIME).set(System.currentTimeMillis())
                        .property(property.getName().replace("-", "_")).set(property.getValue());
                List<String> propIncludes = input.getPropIncludes();
                if (propIncludes != null) {
                    for (int i = 0; i < propIncludes.size(); i++) {
                        String propName = propIncludes.get(i);
                        String propNameU = propName.replace('-', '_');
                        if (message.property(propNameU).value().toObject() == null && entity.getProperty(propName) != null) {
                            message.property(propNameU).set(entity.getProperty(propName).getValue());
                        }
                    }
                }
                sendMessage(message);
                prevValues.put(property.getName(), property.getValue());
            }
        }

        public AllPropertyWatchListener register() {
            Map props = entity.getProperties();
            if (props != null) {
                for (Iterator iter = props.entrySet().iterator(); iter.hasNext(); ) {
                    Property p = (Property) ((Map.Entry) iter.next()).getValue();
                    p.addPropertyWatchListener(this);
                }
            }
            return this;
        }

        public AllPropertyWatchListener deregister() {
            Map props = entity.getProperties();
            if (props != null) {
                for (Iterator iter = props.entrySet().iterator(); iter.hasNext(); ) {
                    Property p = (Property) ((Map.Entry) iter.next()).getValue();
                    p.removePropertyWatchListener(this);
                }
            }
            prevValues.clear();
            return this;
        }
    }
}
