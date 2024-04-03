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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CacheManager {
    static final String PROP_VERBOSE = "swiftmq.store.checkpoint.verbose";
    boolean checkPointVerbose = Boolean.getBoolean(PROP_VERBOSE);
    StoreContext ctx;
    StableStore stableStore;
    TreeMap<Integer, Slot> slots = new TreeMap<>();
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
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/created, minSize=" + minSize + " pages");
    }

    private void panic(Exception e) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/panic: " + e);
        System.err.println("PANIC, EXITING: " + e);
        e.printStackTrace();
        SwiftletManager.getInstance().disableShutdownHook();
        System.exit(-1);
    }

    private void addSlot(Slot newSlot) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/addSlot, slots.size()=" + slots.size());
        slots.put(newSlot.page.pageNo, newSlot);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/addSlot...done.");
    }

    private void evictLRUUnpinnedSlots() {
        int slotsToEvict = slots.size() - minSize;

        if (slotsToEvict > 0) {
            slots.entrySet().stream()
                    .filter(entry -> entry.getValue().pinCount == 0) // Filter unpinned slots
                    .sorted(Map.Entry.comparingByValue(Comparator.comparingLong(Slot::getLastAccessTime))) // Sort by lastAccessTime
                    .limit(slotsToEvict) // Limit to the number of slots to evict
                    .forEach(entry -> {
                        // Optionally: flush if dirty
                        Page page = entry.getValue().page;
                        try {
                            if (page.dirty)
                                stableStore.put(page);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        slots.remove(entry.getKey()); // Remove the slot from the TreeMap
                    });
        }
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
            Slot slot = new Slot();
            slot.page = stableStore.create();
            slot.pinCount = 1;
            slot.lastAccessTime = System.nanoTime();
            addSlot(slot);
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/createAndPin, slot=" + slot);
            return slot.page;
        } finally {
            lock.unlock();
        }
    }

    public Page fetchAndPin(int pageNo) throws Exception {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", toString() + "/fetchAndPin, pageNo=" + pageNo);
            getCount++;
            Slot slot = null;
            if (forceEnsure)
                ensure(pageNo);
            slot = slots.get(pageNo);
            if (slot == null) {
                slot = new Slot();
                slot.page = stableStore.get(pageNo);
                addSlot(slot);
            } else
                hitCount++;
            slot.pinCount++;
            slot.lastAccessTime = System.nanoTime();
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/fetchAndPin, slot=" + slot);
            return slot.page;
        } finally {
            lock.unlock();
        }
    }

    public void unpin(int pageNo) throws Exception {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", toString() + "/unpin, pageNo=" + pageNo + ", nSlots=" + slots.size() + ", maxSize=" + maxSize);
            Slot slot = slots.get(pageNo);
            if (slot == null)
                panic(new Exception("page " + pageNo + " not found in cache, pin before you unpin!"));
            if (slot.pinCount == 0)
                panic(new Exception("page " + pageNo + ": unpin, pinCount is already 0!"));
            slot.pinCount--;
            if (slot.pinCount == 0 && slot.page.dirty && slot.page.empty) {
                // No free pages in the cache!
                stableStore.free(slot.page);
                slots.remove(pageNo);
            }
            if (slots.size() > maxSize)
                evictLRUUnpinnedSlots();
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/unpin, slot=" + slot);
        } finally {
            lock.unlock();
        }
    }

    public void replace(Page page) throws Exception {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/replace, page=" + page);
            Slot slot = slots.get(page.pageNo);
            if (slot == null)
                panic(new Exception("page " + page + " to replace not found in cache!"));
            slot.page = page;
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/replace, slot=" + slot);
        } finally {
            lock.unlock();
        }
    }

    public void flush(List<CheckPointFinishedListener> finishedListeners) throws Exception {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", toString() + "/flush, finishedListeners=" + finishedListeners);
            flush();
            if (finishedListeners != null) {
                for (CheckPointFinishedListener finishedListener : finishedListeners) {
                    finishedListener.checkpointFinished();
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
                System.out.println("CacheManager, hitrate: " + (int) (((double) hitCount / (double) getCount) * 100.0) + "%, fillrate: " + (int) (((double) slots.size() / (double) maxSize) * 100.0) + "%");
            getCount = 0;
            hitCount = 0;
            for (Map.Entry<Integer, Slot> entry : slots.entrySet()) {
                Slot slot = entry.getValue();
                if (slot.page.dirty) {
                    if (slot.page.empty) {
                        if (slot.pinCount == 0) {
                            stableStore.free(slot.page);
                        }
                    } else {
                        stableStore.put(slot.page);
                    }
                }
            }
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
            flush();
        } finally {
            lock.unlock();
        }
    }

    public void close() throws Exception {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/close");
            stableStore.close();
            slots.clear();
        } finally {
            lock.unlock();
        }
    }

    public void reset() throws Exception {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/reset");
            stableStore.reset();
            slots.clear();
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/reset done");
        } finally {
            lock.unlock();
        }
    }

    public String toString() {
        return "CacheManager";
    }
}

