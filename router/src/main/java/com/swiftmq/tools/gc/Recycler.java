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

public abstract class Recycler {
    static final int DEFAULT_SIZE = 128;
    RingBuffer freeList = null;
    Recyclable[] useList = null;
    int maxSize = -1;
    int nUsed = 0;

    public Recycler(int maxSize) {
        this.maxSize = maxSize;
        freeList = new RingBuffer(DEFAULT_SIZE);
        useList = new Recyclable[DEFAULT_SIZE];
    }

    public Recycler() {
        this(-1);
    }

    protected abstract Recyclable createRecyclable();

    private Recyclable getFirstUsed(Recyclable[] list) {
        for (int i = 0; i < list.length; i++) {
            if (list[i] != null) {
                Recyclable r = list[i];
                list[i] = null;
                return r;
            }
        }
        return null;
    }

    private int setFirstFree(Recyclable[] list, Recyclable recyclable) {
        for (int i = 0; i < list.length; i++) {
            if (list[i] == null) {
                list[i] = recyclable;
                return i;
            }
        }
        return -1;
    }

    public Recyclable checkOut() {
        Recyclable recyclable = null;
        if (freeList.getSize() > 0)
            recyclable = (Recyclable) freeList.remove();
        else {
            recyclable = createRecyclable();
            if (nUsed == useList.length) {
                Recyclable[] r = new Recyclable[useList.length + DEFAULT_SIZE];
                System.arraycopy(useList, 0, r, 0, useList.length);
                useList = r;
            }
        }
        recyclable.setRecycleIndex(setFirstFree(useList, recyclable));
        nUsed++;
        return recyclable;
    }

    public void checkIn(Recyclable recyclable) {
        useList[recyclable.getRecycleIndex()] = null;
        nUsed--;
        recyclable.setRecycleIndex(-1);
        if (maxSize == -1 || freeList.getSize() < maxSize) {
            recyclable.reset();
            freeList.add(recyclable);
        }
    }

    public Recyclable get(int index) {
        return useList[index];
    }

    public Recyclable[] getUseList() {
        return useList;
    }

    public void clear() {
        freeList.clear();
        for (int i = 0; i < useList.length; i++)
            useList[i] = null;
        nUsed = 0;
    }
}
