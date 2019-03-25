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

package com.swiftmq.amqp.v100.client.po;

import com.swiftmq.amqp.v100.client.ConnectionVisitor;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.POVisitor;

public class POOpen extends POObject {
    String containerId;
    long maxFrameSize;
    int maxChannel;
    long idleTimeout;

    public POOpen(Semaphore semaphore, String containerId, long maxFrameSize, int maxChannel, long idleTimeout) {
        super(null, semaphore);
        this.containerId = containerId;
        this.maxFrameSize = maxFrameSize;
        this.maxChannel = maxChannel;
        this.idleTimeout = idleTimeout;
    }

    public String getContainerId() {
        return containerId;
    }

    public long getMaxFrameSize() {
        return maxFrameSize;
    }

    public int getMaxChannel() {
        return maxChannel;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void accept(POVisitor visitor) {
        ((ConnectionVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[POOpen, containerId=" + containerId + ", maxFrameSize=" + maxFrameSize + ", maxChannel=" + maxChannel + ", idleTimeout=" + idleTimeout + "]";
    }
}