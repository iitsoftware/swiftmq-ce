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

package com.swiftmq.swiftlet.monitor;

import com.swiftmq.mgmt.Property;
import com.swiftmq.mgmt.PropertyChangeException;
import com.swiftmq.mgmt.PropertyChangeListener;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.timer.event.TimerListener;

public class MemoryMonitor implements TimerListener, PropertyChangeListener {
    SwiftletContext ctx = null;
    Property gcIntervalProp = null;
    Property gcStartProp = null;
    long gcInterval = -1;
    GCTimer gcTimer = null;
    boolean thresholdReached = false;

    public MemoryMonitor(SwiftletContext ctx) {
        this.ctx = ctx;
        gcIntervalProp = ctx.root.getEntity("memory").getProperty("gc-interval");
        gcStartProp = ctx.root.getEntity("memory").getProperty("memory-threshold");
        gcInterval = ((Long) gcIntervalProp.getValue()).longValue();
        gcIntervalProp.setPropertyChangeListener(this);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/created");
    }

    public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
        long newInterval = ((Long) newValue).longValue();
        intervalChange(gcInterval, newInterval);
        gcInterval = newInterval;
    }

    private void intervalChange(long oldInterval, long newInterval) {
        if (oldInterval > 0 && gcTimer != null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/intervalChange (" + oldInterval + "), stopping GC Timer");
            ctx.timerSwiftlet.removeTimerListener(gcTimer);
            gcTimer = null;
        }
        if (newInterval > 0 && thresholdReached) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/intervalChange (" + newInterval + "), starting GC Timer");
            gcTimer = new GCTimer();
            ctx.timerSwiftlet.addTimerListener(newInterval, gcTimer);
        }
    }

    public void performTimeAction() {
        long total = Runtime.getRuntime().totalMemory() / (1024 * 1024);
        long free = Runtime.getRuntime().freeMemory() / (1024 * 1024);
        long max = Runtime.getRuntime().maxMemory() / (1024 * 1024);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/performTimeAction, total=" + total + ", free=" + free + ", max=" + max);
        int th = ((Integer) gcStartProp.getValue()).intValue();
        if (total <= th && th != -1) {
            if (thresholdReached) {
                thresholdReached = false;
                ctx.logSwiftlet.logWarning(ctx.swiftlet.getName(), toString() + "/Memory Usage back to NORMAL! Total=" + total + " MB, free=" + free + " MB, max=" + max + " MB");
                if (gcTimer != null) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/performTimeAction, stopping GC Timer");
                    ctx.timerSwiftlet.removeTimerListener(gcTimer);
                    gcTimer = null;
                    ctx.mailGenerator.generateMail("Memory Monitor on " + SwiftletManager.getInstance().getRouterName() + ": Memory Usage back to NORMAL! Stopping GC Timer");
                } else {
                    ctx.mailGenerator.generateMail("Memory Monitor on " + SwiftletManager.getInstance().getRouterName() + ": Memory Usage back to NORMAL!");
                }
            }
        } else {
            if (!thresholdReached) {
                thresholdReached = true;
                ctx.logSwiftlet.logWarning(ctx.swiftlet.getName(), toString() + "/Memory Usage CRITICAL! Total=" + total + " MB, free=" + free + " MB, max=" + max + " MB");
                if (gcInterval > 0) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/performTimeAction, starting GC Timer");
                    gcTimer = new GCTimer();
                    ctx.timerSwiftlet.addTimerListener(gcInterval, gcTimer);
                    ctx.mailGenerator.generateMail("Memory Monitor on " + SwiftletManager.getInstance().getRouterName() + ": Memory Usage CRITICAL! Starting GC Timer");
                } else {
                    ctx.mailGenerator.generateMail("Memory Monitor on " + SwiftletManager.getInstance().getRouterName() + ": Memory Usage CRITICAL!");
                }
            }
        }
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/close");
        gcIntervalProp.setPropertyChangeListener(null);
        if (gcTimer != null) {
            ctx.timerSwiftlet.removeTimerListener(gcTimer);
            gcTimer = null;
        }
    }

    public String toString() {
        return "MemoryMonitor";
    }

    private class GCTimer implements TimerListener {
        public void performTimeAction() {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.swiftlet.getName(), MemoryMonitor.this.toString() + "/GCTimer/performTimeAction, gc");
            System.gc();
        }
    }
}
