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

package com.swiftmq.tools.gc;

import com.swiftmq.tools.collection.RingBuffer;

public abstract class ObjectRecycler {
    static final int DEFAULT_SIZE = 128;
    RingBuffer freeList = null;
    int maxSize = -1;

    public ObjectRecycler(int maxSize) {
        this.maxSize = maxSize;
        freeList = new RingBuffer(DEFAULT_SIZE);
    }

    public ObjectRecycler() {
        this(-1);
    }

    protected abstract Object createRecyclable();

    public Object checkOut() {
        Object recyclable = null;
        if (freeList.getSize() > 0)
            recyclable = freeList.remove();
        else
            recyclable = createRecyclable();
        return recyclable;
    }

    public void checkIn(Object recyclable) {
        if (maxSize == -1 || freeList.getSize() < maxSize)
            freeList.add(recyclable);
    }

    public int getSize() {
        return freeList.getSize();
    }

    public void clear() {
        freeList.clear();
    }
}
