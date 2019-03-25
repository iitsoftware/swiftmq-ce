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

package com.swiftmq.jms.v610.po;

import com.swiftmq.jms.v610.RecreatableConnection;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.POVisitor;

public class POReconnect extends POObject {
    RecreatableConnection recreatableConnection = null;
    boolean internalRetry = false;
    boolean reconnectAlreadyInProgress = false;

    public POReconnect(Semaphore semaphore, RecreatableConnection recreatableConnection) {
        super(null, semaphore);
        this.recreatableConnection = recreatableConnection;
    }

    public POReconnect(Semaphore semaphore, RecreatableConnection recreatableConnection, boolean internalRetry) {
        super(null, semaphore);
        this.recreatableConnection = recreatableConnection;
        this.internalRetry = internalRetry;
    }

    public RecreatableConnection getRecreatableConnection() {
        return recreatableConnection;
    }

    public boolean isInternalRetry() {
        return internalRetry;
    }

    public boolean isReconnectAlreadyInProgress() {
        return reconnectAlreadyInProgress;
    }

    public void setReconnectAlreadyInProgress(boolean reconnectAlreadyInProgress) {
        this.reconnectAlreadyInProgress = reconnectAlreadyInProgress;
    }

    public void accept(POVisitor visitor) {
        ((ReconnectVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[POReconnect, recreatableConnection=" + recreatableConnection + "]";
    }
}
