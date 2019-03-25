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

package com.swiftmq.tools.collection;

public class LongRingBuffer {
    private long[] elements;
    private int first = 0;
    private int size = 0;
    private int extendSize = 32;

    public LongRingBuffer(int extendSize) {
        this.extendSize = extendSize;
        elements = new long[extendSize];
    }

    public LongRingBuffer(LongRingBuffer base) {
        extendSize = base.extendSize;
        first = base.first;
        size = base.size;
        elements = new long[base.extendSize];
        System.arraycopy(base.elements, 0, elements, 0, base.elements.length);
    }

    public void add(long obj) {
        if (size == elements.length) {
            int newSize = elements.length + extendSize;
            long[] newElements = new long[newSize];
            int n = elements.length - first;
            System.arraycopy(elements, first, newElements, 0, n);
            if (first != 0)
                System.arraycopy(elements, 0, newElements, n, first);
            elements = newElements;
            first = 0;
        }
        elements[(first + size) % elements.length] = obj;
        size++;
    }

    public long remove() {
        long obj = elements[first];
        first++;
        size--;
        if (first == elements.length)
            first = 0;
        return obj;
    }

    public int getSize() {
        return size;
    }

    public void clear() {
        first = 0;
        size = 0;
    }
}
