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

package com.swiftmq.impl.store.standard.cache;


public class Slot implements Comparable {
    int pinCount = 0;
    Page page = null;
    long lastAccessTime = 0;

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public int compareTo(Object that) {
        Slot thatSlot = (Slot) that;
        return Long.compare(lastAccessTime, thatSlot.lastAccessTime);
    }

    public String toString() {
        return "[Slot, pinCount=" + lastAccessTime + ", accessTime=" + lastAccessTime + ", page=" + page + "]";
    }
}

