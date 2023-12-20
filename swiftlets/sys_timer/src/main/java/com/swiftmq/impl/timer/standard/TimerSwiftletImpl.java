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

package com.swiftmq.impl.timer.standard;

import com.swiftmq.mgmt.Configuration;
import com.swiftmq.mgmt.Property;
import com.swiftmq.mgmt.PropertyChangeAdapter;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.timer.event.SystemTimeChangeListener;
import com.swiftmq.swiftlet.timer.event.TimerListener;

public class TimerSwiftletImpl extends TimerSwiftlet {
    SwiftletContext ctx = null;

    public void addInstantTimerListener(long delay, TimerListener listener) {
        ctx.dispatcher.addTimerListener(delay, null, listener, true, false);
    }

    public void addInstantTimerListener(long delay, TimerListener listener, boolean doNotApplySystemTimeChanges) {
        ctx.dispatcher.addTimerListener(delay, null, listener, true, doNotApplySystemTimeChanges);
    }

    public void addTimerListener(long delay, TimerListener listener) {
        ctx.dispatcher.addTimerListener(delay, null, listener, false, false);
    }

    public void addTimerListener(long delay, TimerListener listener, boolean doNotApplySystemTimeChanges) {
        ctx.dispatcher.addTimerListener(delay, null, listener, false, doNotApplySystemTimeChanges);
    }

    public void removeTimerListener(TimerListener listener) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "removeTimerListener, listener=" + listener);
        ctx.dispatcher.removeTimerListener(listener);
    }

    public void addSystemTimeChangeListener(SystemTimeChangeListener systemTimeChangeListener) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "addSystemTimeChangeListener, systemTimeChangeListener=" + systemTimeChangeListener);
        ctx.dispatcher.addSystemTimeChangeListener(systemTimeChangeListener);
    }

    public void removeSystemTimeChangeListener(SystemTimeChangeListener systemTimeChangeListener) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "removeSystemTimeChangeListener, systemTimeChangeListener=" + systemTimeChangeListener);
        ctx.dispatcher.removeSystemTimeChangeListener(systemTimeChangeListener);
    }

    protected void startup(Configuration config) {
        ctx = new SwiftletContext();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup, ...");
        Property prop = config.getProperty("min-delay");
        ctx.dispatcher.setMinDelay((Long) prop.getValue());
        ctx.threadpoolSwiftlet.runAsyncVirtual(ctx.dispatcher);
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue) {
                ctx.dispatcher.setMinDelay((Long) newValue);
            }
        });
        prop = config.getProperty("max-delay");
        ctx.dispatcher.setMaxDelay((Long) prop.getValue());
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue) {
                ctx.dispatcher.setMaxDelay((Long) newValue);
            }
        });
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup, DONE");
    }

    protected void shutdown() {
        // true when shutdown while standby
        if (ctx == null)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown, ...");
        ctx.dispatcher.close();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown, DONE");
    }

}

