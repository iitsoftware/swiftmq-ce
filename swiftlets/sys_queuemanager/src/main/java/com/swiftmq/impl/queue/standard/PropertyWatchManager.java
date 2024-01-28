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

import com.swiftmq.mgmt.Property;
import com.swiftmq.mgmt.PropertyWatchListener;

import java.util.ArrayList;
import java.util.List;

public class PropertyWatchManager {
    private final List<PropertyWatchEntry> watchListeners = new ArrayList<>();

    public void addPropertyWatchListener(Property property, PropertyWatchListener listener) {
        property.addPropertyWatchListener(listener);
        watchListeners.add(new PropertyWatchEntry(property, listener));
    }

    public void removePropertyWatchListeners() {
        for (PropertyWatchEntry entry : watchListeners) {
            entry.property().removePropertyWatchListener(entry.listener());
        }
        watchListeners.clear();
    }

    public record PropertyWatchEntry(Property property, PropertyWatchListener listener) {
    }
}
