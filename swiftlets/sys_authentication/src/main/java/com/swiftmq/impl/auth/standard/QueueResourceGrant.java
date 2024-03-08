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

package com.swiftmq.impl.auth.standard;

import java.util.concurrent.atomic.AtomicBoolean;

public class QueueResourceGrant extends ResourceGrant {
    final AtomicBoolean receiverGranted = new AtomicBoolean(false);
    final AtomicBoolean senderGranted = new AtomicBoolean(false);
    final AtomicBoolean browserGranted = new AtomicBoolean(false);

    public QueueResourceGrant(String resourceName, boolean receiverGranted, boolean senderGranted, boolean browserGranted) {
        super(resourceName);
        this.receiverGranted.set(receiverGranted);
        this.senderGranted.set(senderGranted);
        this.browserGranted.set(browserGranted);
    }

    public void setReceiverGranted(boolean receiverGranted) {
        this.receiverGranted.set(receiverGranted);
    }

    public boolean isReceiverGranted() {
        return (receiverGranted.get());
    }

    public void setSenderGranted(boolean senderGranted) {
        this.senderGranted.set(senderGranted);
    }

    public boolean isSenderGranted() {
        return (senderGranted.get());
    }

    public void setBrowserGranted(boolean browserGranted) {
        this.browserGranted.set(browserGranted);
    }

    public boolean isBrowserGranted() {
        return (browserGranted.get());
    }

    public String toString() {
        StringBuffer s = new StringBuffer();
        s.append("[QueueResourceGrant, resourceName=");
        s.append(resourceName);
        s.append(", receiverGranted=");
        s.append(receiverGranted.get());
        s.append(", senderGranted=");
        s.append(senderGranted.get());
        s.append(", browserGranted=");
        s.append(browserGranted.get());
        s.append("]");
        return s.toString();
    }
}

