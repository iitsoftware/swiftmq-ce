
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

package com.swiftmq.impl.net.netty.scheduler;

import com.swiftmq.impl.net.netty.SwiftletContext;
import com.swiftmq.swiftlet.net.ConnectorMetaData;
import com.swiftmq.swiftlet.timer.event.TimerListener;

public abstract class TCPConnector implements TimerListener {
    SwiftletContext ctx;
    ConnectorMetaData metaData = null;
    boolean closed = false;

    public TCPConnector(SwiftletContext ctx, ConnectorMetaData metaData) {
        this.ctx = ctx;
        this.metaData = metaData;
    }

    public ConnectorMetaData getMetaData() {
        return metaData;
    }

    public void performTimeAction() {
        connect();
    }

    public void start() {
        connect();
        if (metaData.getRetryInterval() > 0)
            ctx.timerSwiftlet.addTimerListener(metaData.getRetryInterval(), this);
    }

    public abstract void connect();

    public void close() {
        if (closed)
            return;
        closed = true;
        ctx.timerSwiftlet.removeTimerListener(this);
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

