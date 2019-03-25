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

package com.swiftmq.tools.queue;


public class FIFOQueue {
    private Object semEnqueue = new Object();
    private Object semDequeue = new Object();
    private Object[] data;
    private int first = 0;
    private volatile int size = 0;
    private int bucketSize = 32;
    private volatile boolean shouldStop = false;

    /**
     * Erzeugt eine FIFOQueue mit einer definierten BucketSize
     */
    public FIFOQueue(int bucketSize) {
        data = new Object[bucketSize];
        this.bucketSize = bucketSize;
    }

    /**
     * Erzeugt eine FIFOQueue mit einer Default-BucketSize von 32
     */
    public FIFOQueue() {
        this(32);
    }

    public int getActEntries() {
        return size;
    }

    public void setStopped() {
        shouldStop = true;
        synchronized (semEnqueue) {
            semEnqueue.notifyAll();
        }
        synchronized (semDequeue) {
            semDequeue.notify();
        }
    }

    public void enqueue(Object obj) {
        if (shouldStop)
            return;
        synchronized (semEnqueue) {
            if (!shouldStop) {
                synchronized (semDequeue) {
                    if (size == data.length) {
                        int newSize = data.length + bucketSize;
                        Object[] newData = new Object[newSize];
                        int n = data.length - first;
                        System.arraycopy(data, first, newData, 0, n);
                        if (first != 0)
                            System.arraycopy(data, 0, newData, n, first);
                        data = newData;
                        first = 0;
                    }
                    data[(first + size) % data.length] = obj;
                    size++;
                    semDequeue.notify();
                }
            }
        }
    }


    public Object dequeue() {
        Object obj = null;
        synchronized (semDequeue) {
            while (size == 0 && !shouldStop) {
                try {
                    semDequeue.wait();
                } catch (InterruptedException ignored) {
                }
            }
            if (!shouldStop) {
                obj = data[first];
                data[first] = null;
                first++;
                size--;
                if (first == data.length) first = 0;
            } else
                data = null;
        }
        synchronized (semEnqueue) {
            semEnqueue.notify();
        }
        return obj;
    }
}
