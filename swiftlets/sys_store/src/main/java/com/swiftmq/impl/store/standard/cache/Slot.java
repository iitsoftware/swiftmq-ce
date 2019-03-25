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
    boolean latched = false;
    Page page = null;
    long accessCount = 0;

    public synchronized void latch() {
        if (latched) {
            try {
                wait();
            } catch (Exception ignored) {
            }
        }
        latched = true;
    }

    public synchronized void unlatch() {
        latched = false;
        notify();
    }

    public int compareTo(Object that) {
        Slot thatSlot = (Slot) that;
        return accessCount == thatSlot.accessCount ? 0 : accessCount > thatSlot.accessCount ? 1 : -1;
    }

    public String toString() {
        return "[Slot, pinCount=" + pinCount + ", accessCount=" + accessCount + ", latched=" + latched + ", page=" + page + "]";
    }
}

