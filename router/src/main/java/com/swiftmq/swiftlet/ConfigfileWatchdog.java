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

package com.swiftmq.swiftlet;

import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.tools.sql.LikeComparator;
import org.dom4j.Document;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ConfigfileWatchdog implements TimerListener {
    TraceSpace traceSpace = null;
    LogSwiftlet logSwiftlet = null;
    String filename = null;
    long lastModified;
    static List<String> EXCLUDES = new ArrayList<String>();

    static {
        EXCLUDES.add("/%/.metadata");
        EXCLUDES.add("/%/usage");
    }

    static List<String> DELEXCLUDES = new ArrayList<String>();

    static {
        DELEXCLUDES.add("/sys$amqp/declarations/transformer/default-inbound-transformers/0");
        DELEXCLUDES.add("/sys$amqp/declarations/transformer/default-outbound-transformers/0");
    }

    public ConfigfileWatchdog(TraceSpace traceSpace, LogSwiftlet logSwiftlet, String filename) {
        this.traceSpace = traceSpace;
        this.logSwiftlet = logSwiftlet;
        this.filename = filename;
        lastModified = new File(filename).lastModified();
        if (traceSpace.enabled) traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/started");
    }

    private boolean isExcluded(String context) {
        for (int i = 0; i < EXCLUDES.size(); i++) {
            if (LikeComparator.compare(context, EXCLUDES.get(i), '\\'))
                return true;
        }
        return false;
    }

    private boolean isDelExcluded(String context) {
        for (int i = 0; i < DELEXCLUDES.size(); i++) {
            if (LikeComparator.compare(context, DELEXCLUDES.get(i), '\\'))
                return true;
        }
        return false;
    }

    private void applyNewEntities(String context, Entity oldConfig, Entity newConfig) throws Exception {
        if (isExcluded(context))
            return;
        if (traceSpace.enabled)
            traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyNewEntities, context=" + context);
        Map entities = newConfig.getEntities();
        if (entities == null || entities.size() == 0)
            return;
        for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry entry = (Map.Entry) iter.next();
            String name = (String) entry.getKey();
            Entity entity = (Entity) entry.getValue();
            if (oldConfig.getEntity(name) == null) {
                if (traceSpace.enabled)
                    traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyNewEntities, context=" + context + ", entity added=" + entity.getName());
                logSwiftlet.logInformation("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyNewEntities, context=" + context + ", entity added=" + entity.getName());
                oldConfig.addEntity(entity);
            } else
                applyNewEntities(context + "/" + entity.getName(), oldConfig.getEntity(name), entity);
        }
    }

    private void applyDeletedEntities(String context, Entity oldConfig, Entity newConfig) throws Exception {
        if (isExcluded(context) || isDelExcluded(context))
            return;
        if (traceSpace.enabled)
            traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyDeletedEntities, context=" + context);
        Map entities = oldConfig.getEntities();
        if (entities == null || entities.size() == 0)
            return;
        for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry entry = (Map.Entry) iter.next();
            String name = (String) entry.getKey();
            Entity entity = (Entity) entry.getValue();
            if (newConfig.getEntity(name) == null && !isDelExcluded(context + "/" + name)) {
                if (traceSpace.enabled)
                    traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyDeletedEntities, context=" + context + ", entity deleted=" + entity.getName());
                logSwiftlet.logInformation("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyDeletedEntities, context=" + context + ", entity deleted=" + entity.getName());
                oldConfig.removeEntity(entity);
            } else
                applyDeletedEntities(context + "/" + entity.getName(), entity, newConfig.getEntity(name));
        }
    }

    private void applyEntityPropChanges(String context, Entity oldConfig, String name, Entity oldEntity, Entity newEntity) throws Exception {
        if (isExcluded(context + "/" + name))
            return;
        Map props = oldEntity.getProperties();
        if (props != null && props.size() > 0) {
            for (Iterator propIter = props.entrySet().iterator(); propIter.hasNext(); ) {
                Property oldProp = (Property) ((Map.Entry) propIter.next()).getValue();
                Property newProp = newEntity.getProperty(oldProp.getName());
                if (traceSpace.enabled)
                    traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context + ", entity=" + name + ", property=" + oldProp.getName());
                if ((oldProp.getValue() != null || newProp.getValue() != null) &&
                        !oldProp.getValue().equals(newProp.getValue())) {
                    if (oldProp.isReadOnly()) {
                        if (oldConfig == null)
                            continue;
                        if (traceSpace.enabled)
                            traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context + ", entity=" + name + ", property=" + oldProp.getName() + ", new value=" + newProp.getValue());
                        logSwiftlet.logInformation("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context + ", entity=" + name + ", property=" + oldProp.getName() + ", new value=" + newProp.getValue());
                        if (traceSpace.enabled)
                            traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context + ", entity=" + name + ", property=" + oldProp.getName() + ", property is read-only, recreate entity");
                        logSwiftlet.logInformation("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context + ", entity=" + name + ", property=" + oldProp.getName() + ", property is read-only, recreate entity");
                        // Reinsert whole entity
                        oldConfig.removeEntity(oldEntity);
                        oldProp.setReadOnly(false);
                        oldProp.setValue(newProp.getValue());
                        oldProp.setReadOnly(true);
                        oldConfig.addEntity(oldEntity);
                    } else {
                        try {
                            if (traceSpace.enabled)
                                traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context + ", entity=" + name + ", property=" + oldProp.getName() + ", new value=" + newProp.getValue());
                            logSwiftlet.logInformation("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context + ", entity=" + name + ", property=" + oldProp.getName() + ", new value=" + newProp.getValue());
                            oldProp.setValue(newProp.getValue());
                        } catch (PropertyChangeException e) {
                            // 1. Check for enabled prop
                            Property enabledProp = oldEntity.getProperty("enabled");
                            if (enabledProp != null && ((Boolean) enabledProp.getValue()).booleanValue()) {
                                try {
                                    if (traceSpace.enabled)
                                        traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context + ", entity=" + name + ", property=" + oldProp.getName() + ", try setting 'enabled' property to false and set the value...");
                                    enabledProp.setValue(false);
                                    oldProp.setValue(newProp.getValue());
                                    enabledProp.setValue(true);
                                    if (traceSpace.enabled)
                                        traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context + ", entity=" + name + ", property=" + oldProp.getName() + ", worked!");
                                } catch (PropertyChangeException e1) {
                                    if (traceSpace.enabled)
                                        traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context + ", entity=" + name + ", property=" + oldProp.getName() + ", got PropertyChangeException: " + e1.getMessage());
                                    logSwiftlet.logInformation("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context + ", entity=" + name + ", property=" + oldProp.getName() + ", got PropertyChangeException: " + e1.getMessage());
                                } catch (Exception e1) {
                                    if (traceSpace.enabled)
                                        traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context + ", entity=" + name + ", property=" + oldProp.getName() + ", got exception: " + e1.getMessage());
                                    logSwiftlet.logError("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context + ", entity=" + name + ", property=" + oldProp.getName() + ", got exception: " + e1.getMessage());
                                }
                            } else {
                                if (traceSpace.enabled)
                                    traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context + ", entity=" + name + ", property=" + oldProp.getName() + ", got PropertyChangeException: " + e.getMessage());
                                logSwiftlet.logInformation("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context + ", entity=" + name + ", property=" + oldProp.getName() + ", got PropertyChangeException: " + e.getMessage());
                            }
                        } catch (Exception e) {
                            if (traceSpace.enabled)
                                traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context + ", entity=" + name + ", property=" + oldProp.getName() + ", got exception: " + e.getMessage());
                            logSwiftlet.logError("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context + ", entity=" + name + ", property=" + oldProp.getName() + ", got exception: " + e.getMessage());
                        }
                    }
                    if (oldProp.isRebootRequired()) {
                        if (traceSpace.enabled)
                            traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context + ", entity=" + name + ", property=" + oldProp.getName() + ", property change requires a reboot!");
                        logSwiftlet.logInformation("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context + ", entity=" + name + ", property=" + oldProp.getName() + ", property change requires a reboot!");
                        logSwiftlet.logWarning("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context + ", entity=" + name + ", property=" + oldProp.getName() + ", property change requires a reboot!");
                    }
                }
            }
        }
    }

    private void applyPropertyChanges(String context, Entity oldConfig, Entity newConfig) throws Exception {
        if (isExcluded(context))
            return;
        if (traceSpace.enabled)
            traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/performTimeAction/applyPropertyChanges, context=" + context);
        applyEntityPropChanges(context, null, oldConfig.getName(), oldConfig, newConfig);
        Map entities = oldConfig.getEntities();
        if (entities == null || entities.size() == 0)
            return;
        for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry entry = (Map.Entry) iter.next();
            String name = (String) entry.getKey();
            Entity oldEntity = (Entity) entry.getValue();
            Entity newEntity = newConfig.getEntity(name);
            if (newEntity == null)
                break;
            applyEntityPropChanges(context, oldConfig, name, oldEntity, newEntity);
            applyPropertyChanges(context + "/" + name, oldEntity, newEntity);
        }
    }

    public void performTimeAction() {
        File f = new File(filename);
        long newLastModified = f.lastModified();
        if (newLastModified == lastModified) {
            if (traceSpace.enabled)
                traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/performTimeAction/no change detected");
            return;
        }
        try {
            if (traceSpace.enabled)
                traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/performTimeAction/change detected");
            Document newConfigfile = XMLUtilities.createDocument(new FileInputStream(f));
            Map configurations = RouterConfiguration.Singleton().getConfigurations();
            for (Iterator iter = configurations.entrySet().iterator(); iter.hasNext(); ) {
                Map.Entry entry = (Map.Entry) iter.next();
                String swiftletName = (String) entry.getKey();
                if (swiftletName.equals(".env"))
                    continue;
                if (traceSpace.enabled)
                    traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/performTimeAction/checking swiftlet=" + swiftletName);
                Swiftlet swiftlet = SwiftletManager.getInstance()._getSwiftlet(swiftletName);
                if (swiftlet != null && swiftlet.getState() == Swiftlet.STATE_ACTIVE) {
                    Configuration config = (Configuration) entry.getValue();
                    Configuration newConfig = SwiftletManager.getInstance().fillConfigurationFromTemplate(swiftletName, newConfigfile);
                    if (newConfig != null) {
                        applyNewEntities("/" + swiftletName, config, newConfig);
                        applyDeletedEntities("/" + swiftletName, config, newConfig);
                        applyPropertyChanges("/" + swiftletName, config, newConfig);
                    }
                }
            }
            lastModified = newLastModified;
        } catch (Exception e) {
            if (traceSpace.enabled)
                traceSpace.trace("SwiftletManager", "ConfigfileWatchdog/performTimeAction, got exception: " + e.getMessage());
            logSwiftlet.logError("SwiftletManager", "ConfigfileWatchdog/performTimeAction, got exception: " + e.getMessage());
        }
    }
}
