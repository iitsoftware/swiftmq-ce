/*
 * Copyright 2024 IIT Software GmbH
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

package com.swiftmq.impl.queue.standard;

import com.swiftmq.jms.MessageImpl;
import com.swiftmq.swiftlet.queue.WireTap;
import com.swiftmq.swiftlet.queue.WireTapSubscriber;

import java.util.HashMap;
import java.util.Map;

public class WireTapManager {
    private final Map<String, WireTap> wireTaps = new HashMap<>();

    public void addWireTapSubscriber(String name, WireTapSubscriber subscriber) {
        WireTap wiretap = wireTaps.get(name);
        if (wiretap == null) {
            wiretap = new WireTap(name, subscriber);
            wireTaps.put(name, wiretap);
        } else {
            wiretap.addSubscriber(subscriber);
        }
    }

    public void removeWireTapSubscriber(String name, WireTapSubscriber subscriber) {
        WireTap wiretap = wireTaps.get(name);
        if (wiretap != null) {
            wiretap.removeSubscriber(subscriber);
            if (!wiretap.hasSubscribers()) {
                wireTaps.remove(name);
            }
        }
    }

    public void forwardWireTaps(MessageImpl message) {
        if (wireTaps.isEmpty()) {
            return;
        }
        wireTaps.forEach((key, value) -> value.putMessage(message));
    }

    public void reset() {
        wireTaps.clear();
    }
}
