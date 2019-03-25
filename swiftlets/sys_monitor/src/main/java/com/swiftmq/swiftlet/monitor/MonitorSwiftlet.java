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

/*
 * Copyright (c) 2000-2005 IIT GmbH, Bremen/Germany. All Rights Reserved.
 *
 * IIT grants you ("Licensee") a non-exclusive, royalty free, license to use,
 * and modify this software, provided that i) this copyright notice and license
 * appear on all copies of the software; and ii) Licensee does not utilize
 * the software in a manner which is disparaging to IIT.
 *
 * This software is provided "AS IS," without a warranty of any kind. ALL
 * EXPRESS OR IMPLIED CONDITIONS, REPRESENTATIONS AND WARRANTIES, INCLUDING ANY
 * IMPLIED WARRANTY OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE OR
 * NON-INFRINGEMENT, ARE HEREBY EXCLUDED. IIT AND ITS LICENSORS SHALL NOT BE
 * LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING
 * OR DISTRIBUTING THE SOFTWARE OR ITS DERIVATIVES. IN NO EVENT WILL IIT OR ITS
 * LICENSORS BE LIABLE FOR ANY LOST REVENUE, PROFIT OR DATA, OR FOR DIRECT,
 * INDIRECT, SPECIAL, CONSEQUENTIAL, INCIDENTAL OR PUNITIVE DAMAGES, HOWEVER
 * CAUSED AND REGARDLESS OF THE THEORY OF LIABILITY, ARISING OUT OF THE USE OF
 * OR INABILITY TO USE SOFTWARE, EVEN IF IIT HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGES.
 *
 * This software is not designed or intended for use in on-line control of
 * aircraft, air traffic, aircraft navigation or aircraft communications; or in
 * the design, construction, operation or maintenance of any nuclear
 * facility. Licensee represents and warrants that it will not use or
 * redistribute the Software for such purposes.
 */

import com.swiftmq.mgmt.Configuration;
import com.swiftmq.mgmt.Property;
import com.swiftmq.mgmt.PropertyChangeException;
import com.swiftmq.mgmt.PropertyChangeListener;
import com.swiftmq.swiftlet.Swiftlet;
import com.swiftmq.swiftlet.SwiftletException;

/**
 * This Swiftlet monitors Connection/Memory Usage and Queue Sizes
 *
 * @author IIT GmbH, Bremen/Germany
 */

public class MonitorSwiftlet extends Swiftlet implements PropertyChangeListener {
    protected SwiftletContext ctx = null;
    Property memoryMonitorIntervalProp = null;
    long memoryMonitorInterval = -1;
    MemoryMonitor memoryMonitor = null;
    Property queueMonitorIntervalProp = null;
    long queueMonitorInterval = -1;
    QueueMonitor queueMonitor = null;
    Property connectionMonitorIntervalProp = null;
    long connectionMonitorInterval = -1;
    ConnectionMonitor connectionMonitor = null;

    public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
        long newInterval = ((Long) newValue).longValue();
        if (property.getName().equals("memory-monitor-interval")) {
            memoryMonitorIntervalChange(memoryMonitorInterval, newInterval);
            memoryMonitorInterval = newInterval;
        } else if (property.getName().equals("queue-monitor-interval")) {
            queueMonitorIntervalChange(queueMonitorInterval, newInterval);
            queueMonitorInterval = newInterval;
        } else if (property.getName().equals("connection-monitor-interval")) {
            connectionMonitorIntervalChange(connectionMonitorInterval, newInterval);
            connectionMonitorInterval = newInterval;
        }
    }

    private void memoryMonitorIntervalChange(long oldInterval, long newInterval) {
        if (oldInterval > 0) {
            ctx.timerSwiftlet.removeTimerListener(memoryMonitor);
            memoryMonitor.close();
            memoryMonitor = null;
        }
        if (newInterval > 0) {
            memoryMonitor = new MemoryMonitor(ctx);
            ctx.timerSwiftlet.addTimerListener(newInterval, memoryMonitor);
        }
    }

    private void queueMonitorIntervalChange(long oldInterval, long newInterval) {
        if (oldInterval > 0) {
            ctx.timerSwiftlet.removeTimerListener(queueMonitor);
            queueMonitor.close();
            queueMonitor = null;
        }
        if (newInterval > 0) {
            queueMonitor = new QueueMonitor(ctx);
            ctx.timerSwiftlet.addTimerListener(newInterval, queueMonitor);
        }
    }

    private void connectionMonitorIntervalChange(long oldInterval, long newInterval) {
        if (oldInterval > 0) {
            ctx.timerSwiftlet.removeTimerListener(connectionMonitor);
            connectionMonitor.close();
            connectionMonitor = null;
        }
        if (newInterval > 0) {
            connectionMonitor = new ConnectionMonitor(ctx);
            ctx.timerSwiftlet.addTimerListener(newInterval, connectionMonitor);
        }
    }

    protected void startup(Configuration configuration) throws SwiftletException {
        ctx = new SwiftletContext(this, configuration);

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup ...");
        memoryMonitorIntervalProp = ctx.root.getEntity("memory").getProperty("memory-monitor-interval");
        memoryMonitorInterval = ((Long) memoryMonitorIntervalProp.getValue()).longValue();
        memoryMonitorIntervalProp.setPropertyChangeListener(this);
        memoryMonitorIntervalChange(-1, memoryMonitorInterval);
        queueMonitorIntervalProp = ctx.root.getEntity("queue").getProperty("queue-monitor-interval");
        queueMonitorInterval = ((Long) queueMonitorIntervalProp.getValue()).longValue();
        queueMonitorIntervalProp.setPropertyChangeListener(this);
        queueMonitorIntervalChange(-1, queueMonitorInterval);
        connectionMonitorIntervalProp = ctx.root.getEntity("connection").getProperty("connection-monitor-interval");
        connectionMonitorInterval = ((Long) connectionMonitorIntervalProp.getValue()).longValue();
        connectionMonitorIntervalProp.setPropertyChangeListener(this);
        connectionMonitorIntervalChange(-1, connectionMonitorInterval);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup done.");

    }

    protected void shutdown() throws SwiftletException {
        if (ctx == null)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown ...");
        connectionMonitorIntervalChange(connectionMonitorInterval, -1);
        connectionMonitorIntervalProp.setPropertyChangeListener(null);
        memoryMonitorIntervalChange(memoryMonitorInterval, -1);
        memoryMonitorIntervalProp.setPropertyChangeListener(null);
        queueMonitorIntervalChange(queueMonitorInterval, -1);
        queueMonitorIntervalProp.setPropertyChangeListener(null);
        ctx.close();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown done.");
    }
}
