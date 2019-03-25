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


public abstract class SimpleQueue extends Thread {
    private Object semEnqueue = new Object();
    private Object semDequeue = new Object();
    private Object[] data;
    private int first;
    private volatile int size;
    private int maxEntries = 10;
    private boolean shouldStop = false;

    /**
     * Erzeugt eine SimpleQueue mit einer max. QueueSize
     */
    public SimpleQueue(int max) {
        data = new Object[max];
        maxEntries = max;
        start();
    }

    /**
     * Erzeugt eine SimpleQueue mit einer Default-QueueSize von 10
     */
    public SimpleQueue() {
        this(10);
    }

    /**
     * Setzt die QueueSize
     */
    public void setMaxEntries(int m) {
        if (m > maxEntries) {
            synchronized (semEnqueue) {
                Object[] newData = new Object[m];
                System.arraycopy(data, 0, newData, 0, maxEntries);
                data = newData;
                maxEntries = m;
            }
        }
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

    /**
     * Steuert ein Objekt in die Verarbeitung ein
     */
    public void putObject(Object obj) {
        if (shouldStop)
            return;
        synchronized (semEnqueue) {
            while (size == data.length && !shouldStop) {
                try {
                    semEnqueue.wait();
                } catch (Exception e) {
                }
            }
            if (!shouldStop) {
                synchronized (semDequeue) {
                    data[(first + size) % data.length] = obj;
                    size++;
                    semDequeue.notify();
                }
            }
        }
    }

    /**
     * Verarbeitet ein Objekt. Wird false zurueckgegeben, wird der Queue-Thread gestoppt.
     */
    public abstract boolean processObject(Object obj);

    public void run() {
        boolean ok = true;
        while (ok) {
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
            if (!shouldStop)
                ok = processObject(obj);
            else
                ok = false;
            obj = null;
        }
    }
}
