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

import com.swiftmq.impl.store.standard.StoreContext;
import com.swiftmq.impl.store.standard.log.CheckPointFinishedListener;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.tools.collection.IntRingBuffer;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CacheManager {
    static final String PROP_VERBOSE = "swiftmq.store.checkpoint.verbose";
    boolean checkPointVerbose = Boolean.getBoolean(PROP_VERBOSE);
    StoreContext ctx;
    StableStore stableStore;
    Slot[] slots = null;
    IntRingBuffer freePages = new IntRingBuffer(256);
    int nSlots = 0;
    int minSize;
    int maxSize;
    boolean forceEnsure = false;
    int getCount = 0;
    int hitCount = 0;
    Lock lock = new ReentrantLock();

    public CacheManager(StoreContext ctx, StableStore stableStore, int minSize, int maxSize) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/create, min:max=" + minSize + ":" + maxSize);
        this.ctx = ctx;
        this.stableStore = stableStore;
        this.minSize = minSize;
        this.maxSize = maxSize;
        init();
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/created, minSize=" + minSize + " pages");
    }

    private void init() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/init ...");
        slots = new Slot[maxSize];
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/init done");
    }

    private void throwException(String msg) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/throwException: " + msg);
        throw new Exception(msg);
    }

    private void panic(Exception e) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/panic: " + e);
        System.err.println("PANIC, EXITING: " + e);
        e.printStackTrace();
        SwiftletManager.getInstance().disableShutdownHook();
        System.exit(-1);
    }

    private Slot getSlot(int pageNo) {
        if (pageNo < slots.length)
            return slots[pageNo];
        return null;
    }

    private void addSlot(int pageNo, Slot slot) {
        if (pageNo >= slots.length) {
            Slot[] s = new Slot[pageNo + 128];
            System.arraycopy(slots, 0, s, 0, slots.length);
            slots = s;
        }
        slots[pageNo] = slot;
        nSlots++;
    }

    private void removeSlot(int pageNo) {
        slots[pageNo] = null;
        nSlots--;
    }

    private void freeSlots() throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/freeSlots (before), size=" + nSlots);
        int size = slots.length;
        for (int i = size - 1; i >= 0; i--) {
            Slot slot = slots[i];
            if (slot != null) {
                if (slot.pinCount == 0) {
                    if (slot.page.dirty) {
                        if (slot.page.empty)
                            stableStore.free(slot.page);
                        else
                            stableStore.put(slot.page);
                    }
                    removeSlot(i);
                }
            }
        }
        freePages.clear();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/freeSlots (after), size=" + nSlots);
    }

    private void addSlot(Slot newSlot) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/addSlot, slots.size()=" + nSlots);
        addSlot(newSlot.page.pageNo, newSlot);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/addSlot...done.");
    }

    public void setForceEnsure(boolean forceEnsure) {
        this.forceEnsure = forceEnsure;
    }

    public void ensure(int pageNo) throws Exception {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/ensure, pageNo=" + pageNo);
            stableStore.ensure(pageNo);
        } finally {
            lock.unlock();
        }
    }

    public Page createAndPin() throws Exception {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/createAndPin...");
            getCount++;
            Slot slot = null;
            if (freePages.getSize() == 0) {
                slot = new Slot();
                slot.page = stableStore.create();
                addSlot(slot);
            } else {
                hitCount++;
                int pageNo = freePages.remove();
                slot = getSlot(pageNo);
                if (slot == null)
                    panic(new Exception("slot for page " + pageNo + " not found in cache (createAndPin)!"));
                System.arraycopy(stableStore.emptyData, 0, slot.page.data, 0, stableStore.emptyData.length);
            }
            slot.pinCount = 1;
            slot.accessCount = 1;
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/createAndPin, slot=" + slot);
            return slot.page;
        } finally {
            lock.unlock();
        }
    }

    /**
     * @param pageNo
     */
    public Page fetchAndPin(int pageNo) throws Exception {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", toString() + "/fetchAndPin, pageNo=" + pageNo);
            getCount++;
            Slot slot = null;
            if (forceEnsure)
                ensure(pageNo);
            slot = getSlot(pageNo);
            if (slot == null) {
                slot = new Slot();
                slot.page = stableStore.get(pageNo);
                addSlot(slot);
            } else
                hitCount++;
            slot.pinCount++;
            slot.accessCount++;
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/fetchAndPin, slot=" + slot);
            return slot.page;
        } finally {
            lock.unlock();
        }
    }

    /**
     * @param pageNo
     */
    public void latch(int pageNo) throws Exception {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/latch, pageNo=" + pageNo);
            Slot slot = getSlot(pageNo);
            if (slot == null)
                panic(new Exception("page " + pageNo + " not found in cache, pin before you latch!"));
            slot.latch();
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/latch, slot=" + slot);
        } finally {
            lock.unlock();
        }
    }

    /**
     * @param pageNo
     */
    public void unlatch(int pageNo) throws Exception {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/unlatch, pageNo=" + pageNo);
            Slot slot = getSlot(pageNo);
            if (slot == null)
                panic(new Exception("page " + pageNo + " not found in cache, pin before you unlatch!"));
            slot.unlatch();
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/unlatch, slot=" + slot);
        } finally {
            lock.unlock();
        }
    }

    /**
     * @param pageNo
     */
    public void unpin(int pageNo) throws Exception {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", toString() + "/unpin, pageNo=" + pageNo + ", nSlots=" + nSlots + ", maxSize=" + maxSize);
            Slot slot = getSlot(pageNo);
            if (slot == null)
                panic(new Exception("page " + pageNo + " not found in cache, pin before you unpin!"));
            if (slot.pinCount == 0)
                panic(new Exception("page " + pageNo + ": unpin, pinCount is already 0!"));
            slot.pinCount--;
            if (slot.pinCount == 0) {
                if (slot.page.dirty) {
                    if (nSlots > maxSize) {
                        if (slot.page.empty) {
                            stableStore.free(slot.page);
                        } else {
                            stableStore.put(slot.page);
                        }
                        removeSlot(pageNo);
                    } else {
                        if (slot.page.empty) {
                            freePages.add(pageNo);
                        }
                    }
                } else if (nSlots > maxSize)
                    removeSlot(pageNo);
            }
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/unpin, slot=" + slot);
        } finally {
            lock.unlock();
        }
    }

    public void replace(Page page) throws Exception {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/replace, page=" + page);
            Slot slot = getSlot(page.pageNo);
            if (slot == null)
                panic(new Exception("page " + page + " to replace not found in cache!"));
            slot.page = page;
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/replace, slot=" + slot);
        } finally {
            lock.unlock();
        }
    }

    public void flush(List finishedListeners) throws Exception {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", toString() + "/flush, finishedListeners=" + finishedListeners);
            flush();
            if (finishedListeners != null) {
                for (int i = 0; i < finishedListeners.size(); i++) {
                    ((CheckPointFinishedListener) finishedListeners.get(i)).checkpointFinished();
                }
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", toString() + "/flush, finishedListeners=" + finishedListeners + "...done.");
        } finally {
            lock.unlock();
        }
    }

    public void flush() throws Exception {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/flush...");
            if (checkPointVerbose && getCount > 0)
                System.out.println("CacheManager, hitrate: " + (int) (((double) hitCount / (double) getCount) * 100.0) + "%, fillrate: " + (int) (((double) nSlots / (double) maxSize) * 100.0) + "%, free pages: " + freePages.getSize());
            getCount = 0;
            hitCount = 0;
            int size = slots.length;
            for (int i = size - 1; i >= 0; i--) {
                Slot slot = slots[i];
                if (slot != null) {
                    if (slot.page.dirty) {
                        if (slot.page.empty) {
                            if (slot.pinCount == 0) {
                                stableStore.free(slot.page);
                            }
                        } else {
                            stableStore.put(slot.page);
                        }
                    }
                    if (slot.pinCount == 0 && (slot.page.empty || nSlots > minSize && slot.accessCount == 1)) {
                        removeSlot(slot.page.pageNo);
                    }
                }
            }
            freePages.clear();
            stableStore.sync();
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/flush...done.");
        } finally {
            lock.unlock();
        }
    }

    public void shrink() throws Exception {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/shrink");
            freeSlots();
        } finally {
            lock.unlock();
        }
    }

    public void close() throws Exception {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/close");
            stableStore.close();
            slots = null;
        } finally {
            lock.unlock();
        }
    }

    public void reset() throws Exception {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/reset");
            stableStore.reset();
            slots = null;
            init();
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/reset done");
        } finally {
            lock.unlock();
        }
    }

    public String toString() {
        return "CacheManager";
    }
}

