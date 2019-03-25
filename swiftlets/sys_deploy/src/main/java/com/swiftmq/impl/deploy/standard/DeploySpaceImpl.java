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

package com.swiftmq.impl.deploy.standard;

import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.deploy.DeploySpace;
import com.swiftmq.swiftlet.deploy.event.DeployListener;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.deploy.Bundle;
import com.swiftmq.tools.deploy.BundleEvent;
import com.swiftmq.tools.deploy.DeployPath;
import com.swiftmq.util.SwiftUtilities;

import java.io.File;
import java.util.Date;
import java.util.List;

public class DeploySpaceImpl implements DeploySpace, TimerListener {
    SwiftletContext ctx = null;
    Entity spaceEntity = null;
    DeployPath deployPath = null;
    long checkInterval = -1;
    DeployListener listener = null;
    Entity usage = null;
    EntityList deployList = null;
    boolean closed = false;
    boolean instant = false;

    public DeploySpaceImpl(SwiftletContext ctx, Entity spaceEntity) throws Exception {
        this.ctx = ctx;
        this.spaceEntity = spaceEntity;
        init();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.deploySwiftlet.getName(), toString() + "/created");
    }

    public Bundle[] getInstalledBundles() throws Exception {
        List list = deployPath.getInstalledBundles();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.deploySwiftlet.getName(), toString() + "/getInstalledBundles: " + list);
        if (list == null)
            return null;
        for (int i = 0; i < list.size(); i++)
            createUsage((Bundle) list.get(i));
        return (Bundle[]) list.toArray(new Bundle[list.size()]);
    }

    public synchronized void setDeployListener(DeployListener listener) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.deploySwiftlet.getName(), toString() + "/setDeployListener: " + listener);
        if (listener == null) {
            if (this.listener != null)
                stopTimer();
            this.listener = listener;
        } else {
            this.listener = listener;
            startTimer();
        }
    }

    private void init() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.deploySwiftlet.getName(), toString() + "/init ...");
        Property prop = spaceEntity.getProperty("check-interval");
        checkInterval = ((Long) prop.getValue()).longValue();

        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                if (checkInterval != -1 && listener != null)
                    stopTimer();
                checkInterval = ((Long) newValue).longValue();
                if (checkInterval != -1 && listener != null)
                    startTimer();
            }
        });
        prop = spaceEntity.getProperty("path");
        File f = new File(SwiftUtilities.addWorkingDir((String) prop.getValue()));
        if (!f.exists() || !f.isDirectory())
            throw new Exception("Path is invalid: " + SwiftUtilities.addWorkingDir((String) prop.getValue()));
        deployPath = new DeployPath(f, SwiftletManager.class.getClassLoader());

        // Only on cold start, otherwise the .deleted files are removed!
        if (!ctx.isReboot)
            deployPath.purge();

        usage = ctx.usageList.createEntity();
        usage.setName(spaceEntity.getName());
        usage.createCommands();
        usage.setDynamicObject(this);
        ctx.usageList.addEntity(usage);
        deployList = (EntityList) usage.getEntity("deployments");

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.deploySwiftlet.getName(), toString() + "/init done");
    }

    private void startTimer() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.deploySwiftlet.getName(), toString() + "/startTimer, instant timer listener, 1000");
        instant = true;
        ctx.timerSwiftlet.addInstantTimerListener(1000, this);
    }

    private void stopTimer() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.deploySwiftlet.getName(), toString() + "/stopTimer");
        ctx.timerSwiftlet.removeTimerListener(this);
    }

    private void createUsage(Bundle bundle) {
        try {
            Entity entity = deployList.createEntity();
            entity.setName(bundle.getBundleName());
            entity.createCommands();
            entity.setDynamicObject(bundle);
            entity.getProperty("deploytime").setValue(new Date().toString());
            entity.getProperty("directory").setValue(bundle.getBundleDir());
            deployList.addEntity(entity);
        } catch (Exception e) {
        }
    }

    public synchronized void performTimeAction() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.deploySwiftlet.getName(), toString() + "/performTimeAction ...");
        if (closed)
            return;
        try {
            BundleEvent[] events = deployPath.getBundleEvents();
            if (events != null) {
                for (int i = 0; i < events.length; i++) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.deploySwiftlet.getName(), toString() + "/performTimeAction, event=" + events[i]);
                    Bundle bundle = events[i].getBundle();
                    try {
                        switch (events[i].getType()) {
                            case BundleEvent.BUNDLE_ADDED:
                                ctx.logSwiftlet.logInformation(ctx.deploySwiftlet.getName(), toString() + "/performTimeAction, " + bundle.getBundleName() + ", BundleEvent.BUNDLE_ADDED");
                                listener.bundleAdded(events[i].getBundle());
                                createUsage(events[i].getBundle());
                                break;
                            case BundleEvent.BUNDLE_REMOVED:
                                ctx.logSwiftlet.logInformation(ctx.deploySwiftlet.getName(), toString() + "/performTimeAction, " + bundle.getBundleName() + ", BundleEvent.BUNDLE_REMOVED");
                                listener.bundleRemoved(events[i].getBundle(), false);
                                deployList.removeDynamicEntity(events[i].getBundle());
                                break;
                            case BundleEvent.BUNDLE_CHANGED:
                                ctx.logSwiftlet.logInformation(ctx.deploySwiftlet.getName(), toString() + "/performTimeAction, " + bundle.getBundleName() + ", BundleEvent.BUNDLE_CHANGED");
                                listener.bundleRemoved(events[i].getBundle(), true);
                                deployList.removeDynamicEntity(events[i].getBundle());
                                listener.bundleAdded(events[i].getBundle());
                                createUsage(events[i].getBundle());
                                break;
                        }
                    } catch (Exception e1) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.deploySwiftlet.getName(), toString() + "/performTimeAction, " + bundle.getBundleName() + ", exception: " + e1);
                        ctx.logSwiftlet.logError(ctx.deploySwiftlet.getName(), toString() + "/performTimeAction, " + bundle.getBundleName() + ", exception: " + e1 + ", removing bundle. Correct the error and deploy again!");
                        deployPath.removeBundle(bundle);
                        deployList.removeDynamicEntity(events[i].getBundle());
                    }
                }
            } else if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.deploySwiftlet.getName(), toString() + "/performTimeAction, no events!");

        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.deploySwiftlet.getName(), toString() + "/performTimeAction, exception=" + e);
            ctx.logSwiftlet.logInformation(ctx.deploySwiftlet.getName(), toString() + "/performTimeAction, exception=" + e);
        }
        if (instant) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.deploySwiftlet.getName(), toString() + "/performTimeAction, startTimer, interval=" + checkInterval);
            instant = false;
            ctx.timerSwiftlet.addTimerListener(checkInterval, this);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.deploySwiftlet.getName(), toString() + "/performTimeAction done");
    }

    public synchronized void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.deploySwiftlet.getName(), toString() + "/close ...");
        if (closed)
            return;
        closed = true;
        if (checkInterval != -1 && listener != null)
            stopTimer();
        ctx.usageList.removeDynamicEntity(this);
        ;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.deploySwiftlet.getName(), toString() + "/close done");
    }

    public String toString() {
        return "DeploySpaceImpl, name=" + spaceEntity.getName();
    }
}
