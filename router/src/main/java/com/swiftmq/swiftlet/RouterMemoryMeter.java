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
import com.swiftmq.swiftlet.event.SwiftletManagerAdapter;
import com.swiftmq.swiftlet.event.SwiftletManagerEvent;
import com.swiftmq.swiftlet.mgmt.MgmtSwiftlet;
import com.swiftmq.swiftlet.mgmt.event.MgmtListener;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.timer.event.TimerListener;

public class RouterMemoryMeter implements TimerListener, MgmtListener {
    TimerSwiftlet timerSwiftlet = null;
    MgmtSwiftlet mgmtSwiftlet = null;
    EntityList memoryList = null;
    Property freeMemProp = null;
    Property totalMemProp = null;
    Property memCollectIntervalProp = null;
    volatile long collectInterval = 0;
    volatile boolean collectActive = false;

    public RouterMemoryMeter(Property memCollectIntervalProp) {
        this.memCollectIntervalProp = memCollectIntervalProp;
        collectInterval = ((Long) memCollectIntervalProp.getValue()).longValue();
        memCollectIntervalProp.setPropertyChangeListener(new PropertyChangeListener() {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                collectInterval = ((Long) newValue).longValue();
                timerChange();
            }
        });
    }

    private synchronized void timerChange() {
        if (collectActive) {
            timerSwiftlet.removeTimerListener(this);
            timerSwiftlet.addTimerListener(collectInterval, this);
        }
    }

    public EntityList getMemoryList() {
        if (memoryList == null) {
            try {
                Entity tplEntity = new Entity("router-memory", "Router Memory", "Router Memory", null);
                Property prop = new Property("free-memory");
                prop.setType(Integer.class);
                prop.setDisplayName("Free Memory (KB)");
                prop.setDescription("Free Memory (KB)");
                prop.setDefaultValue(new Integer(0));
                prop.setRebootRequired(false);
                prop.setReadOnly(true);
                prop.setMandatory(true);
                tplEntity.addProperty(prop.getName(), prop);
                prop = new Property("total-memory");
                prop.setType(Integer.class);
                prop.setDisplayName("Total Memory (KB)");
                prop.setDescription("Total Memory (KB)");
                prop.setDefaultValue(new Integer(0));
                prop.setRebootRequired(false);
                prop.setReadOnly(true);
                prop.setMandatory(true);
                tplEntity.addProperty(prop.getName(), prop);
                memoryList = new EntityList("router-memory-list", "Router Memory List", "Router Memory List", null, tplEntity, false);
                memoryList.createCommands();
                CommandExecutor gcExecutor = new CommandExecutor() {
                    public String[] execute(String[] context, Entity entity, String[] cmd) {
                        if (cmd.length > 2)
                            return new String[]{TreeCommands.ERROR, "Invalid command, please try 'gc'"};
                        System.gc();
                        return null;
                    }
                };
                Command gcCommand = new Command("gc", "gc", "Run Garbage Collection", true, gcExecutor, true, true);
                memoryList.getCommandRegistry().addCommand(gcCommand);
                memoryList.setDynamicPropNames(new String[]{"free-memory", "total-memory"});
                memoryList.setDynamic(true);
                Entity entity = memoryList.createEntity();
                entity.setName(SwiftletManager.getInstance().getRouterName());
                entity.createCommands();
                freeMemProp = entity.getProperty("free-memory");
                totalMemProp = entity.getProperty("total-memory");
                memoryList.addEntity(entity);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return memoryList;
    }

    public synchronized void start() {
        timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");
        mgmtSwiftlet = (MgmtSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$mgmt");
        if (mgmtSwiftlet != null)
            mgmtSwiftlet.addMgmtListener(RouterMemoryMeter.this);
        else {
            SwiftletManager.getInstance().addSwiftletManagerListener("sys$mgmt", new SwiftletManagerAdapter() {
                public void swiftletStarted(SwiftletManagerEvent evt) {
                    mgmtSwiftlet = (MgmtSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$mgmt");
                    mgmtSwiftlet.addMgmtListener(RouterMemoryMeter.this);
                }
            });
        }
    }

    public synchronized void adminToolActivated() {
        timerSwiftlet.addTimerListener(collectInterval, this);
        collectActive = true;
    }

    public synchronized void adminToolDeactivated() {
        timerSwiftlet.removeTimerListener(this);
        collectActive = false;
    }

    public void performTimeAction() {
        try {
            freeMemProp.setValue(new Integer((int) Runtime.getRuntime().freeMemory() / 1024));
            totalMemProp.setValue(new Integer((int) Runtime.getRuntime().totalMemory() / 1024));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized void close() {
        if (collectActive)
            timerSwiftlet.removeTimerListener(this);
        if (mgmtSwiftlet != null)
            mgmtSwiftlet.removeMgmtListener(this);
    }
}
