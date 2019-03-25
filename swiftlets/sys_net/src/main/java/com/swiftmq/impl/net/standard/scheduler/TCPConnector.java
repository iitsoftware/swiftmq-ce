
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

package com.swiftmq.impl.net.standard.scheduler;

import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.net.ConnectorMetaData;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.timer.event.TimerListener;

public abstract class TCPConnector implements TimerListener {
    TimerSwiftlet timerSwiftlet = null;
    ConnectorMetaData metaData = null;
    boolean closed = false;

    /**
     * @param metaData
     * @SBGen Constructor assigns metaData
     */
    public TCPConnector(ConnectorMetaData metaData) {
        // SBgen: Assign variable
        this.metaData = metaData;
        timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");
    }

    /**
     * @return
     * @SBGen Method get metaData
     */
    public ConnectorMetaData getMetaData() {
        // SBgen: Get variable
        return (metaData);
    }

    /**
     * Performs the specific time action
     *
     * @param evt timer event
     */
    public void performTimeAction() {
        connect();
    }

    public void start() {
        connect();
        if (metaData.getRetryInterval() > 0)
            timerSwiftlet.addTimerListener(metaData.getRetryInterval(), this);
    }

    public abstract void connect();

    public void close() {
        if (closed)
            return;
        closed = true;
        timerSwiftlet.removeTimerListener(this);
    }

    public String toString() {
        StringBuffer b = new StringBuffer();
        b.append("swiftlet=");
        b.append(metaData.getSwiftlet().getName());
        b.append(", hostname=");
        b.append(metaData.getHostname());
        b.append(", port=");
        b.append(metaData.getPort());
        return b.toString();
    }
}

