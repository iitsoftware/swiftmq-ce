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

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RingBufferThreadsafe extends RingBuffer {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public RingBufferThreadsafe(int extendSize) {
        super(extendSize);
    }

    public RingBufferThreadsafe(RingBuffer base) {
        super(base);
    }

    public void add(Object obj) {
        lock.writeLock().lock();
        try {
            super.add(obj);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Object remove() {
        lock.writeLock().lock();
        try {
            return super.remove();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int getSize() {
        lock.readLock().lock();
        try {
            return super.getSize();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            super.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
