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

package com.swiftmq.impl.mqtt.session;

import com.swiftmq.impl.mqtt.pubsub.SubscriptionStoreEntry;

import java.util.List;

public class SessionStoreEntry {
    int durableId;
    int pid;
    List<SubscriptionStoreEntry> subscriptionStoreEntries;

    public SessionStoreEntry(int durableId, int pid, List<SubscriptionStoreEntry> subscriptionStoreEntries) {
        this.durableId = durableId;
        this.pid = pid;
        this.subscriptionStoreEntries = subscriptionStoreEntries;
    }

    @Override
    public String toString() {
        return "SessionStoreEntry{" +
                "durableId=" + durableId +
                ", pid=" + pid +
                ", subscriptionStoreEntries=" + subscriptionStoreEntries +
                '}';
    }
}
