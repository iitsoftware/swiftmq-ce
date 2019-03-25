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

package com.swiftmq.impl.store.standard.index;

import com.swiftmq.impl.store.standard.StoreContext;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MessagePageReference {
    StoreContext ctx = null;
    int pageNo = 0;
    int refCount = 0;
    int activeRefs = 0;
    Lock lock = new ReentrantLock();
    Condition noMoreActiveRefs = null;

    public MessagePageReference(StoreContext ctx, int pageNo, int refCount) {
        this.ctx = ctx;
        this.pageNo = pageNo;
        this.refCount = refCount;
        noMoreActiveRefs = lock.newCondition();
    }

    public void incRefCount() {
        lock.lock();
        try {
            refCount++;
        } finally {
            lock.unlock();
        }
    }

    public int getRefCount() {
        lock.lock();
        try {
            return refCount;
        } finally {
            lock.unlock();
        }
    }

    public MessagePageReference decRefCountAndMarkActive() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/decRefCountAndMarkActive ...");
        lock.lock();
        try {
            if (refCount == 1) {
                if (activeRefs > 0) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace("sys$store", toString() + "/decRefCountAndMarkActive, await ...");
                    noMoreActiveRefs.awaitUninterruptibly();
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace("sys$store", toString() + "/decRefCountAndMarkActive, released ...");
                }
                return null;
            } else {
                refCount--;
                activeRefs++;
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$store", toString() + "/decRefCountAndMarkActive, marked active");
                return this;
            }
        } finally {
            lock.unlock();
        }
    }

    public void unMarkActive() {
        lock.lock();
        try {
            activeRefs--;
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/unMarkActive");
            if (activeRefs == 0)
                noMoreActiveRefs.signal();
        } finally {
            lock.unlock();
        }
    }

    public String toString() {
        return "[MessagePageReference, pageNo=" + pageNo + ", refCount=" + refCount + ", activeRefs=" + activeRefs + "]";
    }
}
