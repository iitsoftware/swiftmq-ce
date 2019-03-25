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

package com.swiftmq.tools.util;

public class UninterruptableWaiter {
    public static void doWait(Object waiter) {
        boolean wasInterrupted = Thread.interrupted();
        boolean ok = false;
        do {
            try {
                waiter.wait();
                ok = true;
            } catch (InterruptedException e) {
                ok = false;
                wasInterrupted = true;
            }
        } while (!ok);
        if (wasInterrupted)
            Thread.currentThread().interrupt();
    }

    public static void doWait(Object waiter, long timeout) {
        if (timeout == 0) {
            doWait(waiter);
            return;
        }
        boolean wasInterrupted = Thread.interrupted();
        boolean ok = false;
        do {
            long start = System.currentTimeMillis();
            try {
                waiter.wait(timeout);
                ok = true;
            } catch (InterruptedException e) {
                long delta = System.currentTimeMillis() - start;
                timeout -= delta;
                ok = timeout <= 0;
                wasInterrupted = true;
            }
        } while (!ok);
        if (wasInterrupted)
            Thread.currentThread().interrupt();
    }
}
