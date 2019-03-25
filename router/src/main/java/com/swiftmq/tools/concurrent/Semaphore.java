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

package com.swiftmq.tools.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Semaphore {
    boolean notified = false;
    boolean interruptable = true;
    Lock lock = new ReentrantLock();
    Condition waiter = null;
    TimeUnit unit = TimeUnit.MILLISECONDS;

    public Semaphore(boolean interruptable) {
        this.interruptable = interruptable;
        waiter = lock.newCondition();
    }

    public Semaphore() {
        this(true);
    }

    public void waitHere() {
        lock.lock();
        try {
            while (!notified) {
                try {
                    if (interruptable)
                        waiter.await();
                    else
                        waiter.awaitUninterruptibly();
                } catch (Exception ignored) {
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void waitHere(long ms) {
        lock.lock();
        try {
            long nanos = unit.toNanos(ms);
            while (!notified && nanos > 0) {
                try {
                    nanos = waiter.awaitNanos(nanos);
                } catch (Exception ignored) {
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void notifySingleWaiter() {
        lock.lock();
        try {
            notified = true;
            waiter.signal();
        } finally {
            lock.unlock();
        }
    }

    public void notifyAllWaiters() {
        lock.lock();
        try {
            notified = true;
            waiter.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public boolean isNotified() {
        lock.lock();
        try {
            return notified;
        } finally {
            lock.unlock();
        }
    }

    public void reset() {
        lock.lock();
        try {
            notified = false;
        } finally {
            lock.unlock();
        }
    }
}

