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
import com.swiftmq.impl.store.standard.cache.Page;
import com.swiftmq.impl.store.standard.log.DeleteLogAction;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.swiftlet.store.StoreEntry;
import com.swiftmq.tools.collection.RingBuffer;

import java.util.List;

public class QueueIndex extends Index {
    PageInputStream pis = null;
    PageOutputStream pos = null;
    long maxKey = -1;
    RingBuffer mpList = null;

    public QueueIndex(StoreContext ctx, int rootPageNo) {
        super(ctx, rootPageNo);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/create");
        pis = new PageInputStream(ctx);
        pos = new PageOutputStream(ctx);
        mpList = new RingBuffer(32);
        try {
            Long max = (Long) getMaxKey();
            if (max != null)
                maxKey = max.longValue();
            unloadPages();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setJournal(List journal) {
        super.setJournal(journal);
        pos.setJournal(journal);
    }

    public IndexPage createIndexPage(int pageNo) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/createIndexPage, pageNo=" + pageNo);
        IndexPage ip = new QueueIndexPage(ctx, pageNo);
        return ip;
    }

    public StoreEntry get(QueueIndexEntry indexEntry) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/get, indexEntry=" + indexEntry);
        pis.setRootPageNo(indexEntry.getRootPageNo());
        StoreEntry storeEntry = new StoreEntry();
        storeEntry.key = indexEntry;
        storeEntry.priority = indexEntry.getPriority();
        storeEntry.deliveryCount = indexEntry.getDeliveryCount();
        storeEntry.expirationTime = indexEntry.getExpirationTime();
        storeEntry.message = MessageImpl.createInstance(pis.readInt());
        storeEntry.message.readContent(pis);
        pis.unloadPages();
        pis.reset();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/get, returns=" + storeEntry);
        return storeEntry;
    }

    public QueueIndexEntry add(StoreEntry storeEntry) throws Exception {
        return add(storeEntry, false);
    }

    public QueueIndexEntry add(StoreEntry storeEntry, boolean referencable) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/add, storeEntry=" + storeEntry);
        Integer pKey = (Integer) storeEntry.message.getPersistentKey();
        if (pKey == null || !referencable) {
            storeEntry.message.writeContent(pos);
            pos.flush();
            pKey = Integer.valueOf(pos.getRootPageNo());
            if (referencable) {
                storeEntry.message.setPersistentKey(pKey);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$store", toString() + "/add, storeEntry=" + storeEntry + ", referenceable and not yet persistet, new pKey=" + pKey);
            } else {
                storeEntry.message.setPersistentKey(null);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$store", toString() + "/add, storeEntry=" + storeEntry + ", NOT referenceable");
            }
        } else {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", toString() + "/add, storeEntry=" + storeEntry + ", already persistet, old pKey=" + pKey);
        }
        if (referencable) {
            MessagePageReference ref = ctx.referenceMap.getReference(pKey, true);
            ref.incRefCount();
        }
        QueueIndexEntry entry = new QueueIndexEntry();
        entry.setKey(Long.valueOf(++maxKey));
        entry.setRootPageNo(pKey.intValue());
        entry.setPriority(storeEntry.priority);
        entry.setDeliveryCount(storeEntry.deliveryCount);
        entry.setExpirationTime(storeEntry.expirationTime);
        entry.setValid(true);
        pos.reset();
        add(entry);
        storeEntry.key = entry;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/add done, storeEntry=" + storeEntry);
        return entry;
    }

    public MessagePageReference remove(QueueIndexEntry indexEntry) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/remove, indexEntry=" + indexEntry);
        remove(indexEntry.getKey());
        Integer rootPageNo = Integer.valueOf(indexEntry.getRootPageNo());
        MessagePageReference ref = ctx.referenceMap.getReference(rootPageNo, false);
        if (ref != null)
            ref = ref.decRefCountAndMarkActive();
        if (ref == null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", toString() + "/remove, indexEntry=" + indexEntry + ", no more refs, REMOVE message");
            ctx.referenceMap.removeReference(rootPageNo);
            MessagePage mp = new MessagePage(ctx.cacheManager.fetchAndPin(rootPageNo.intValue()));
            int next = -1;
            do {
                mpList.add(mp.page);
                if (mp.page.pageNo == 0) {
                    // This MUST NOT happen after a failover. We just ignore it.
                    ctx.logSwiftlet.logWarning("sys$store", toString() + "/try to delete a message page which was already deleted, ignore. indexEntry: " + indexEntry);
                    break;
                }
                byte[] bi = new byte[mp.getLength()];
                System.arraycopy(mp.page.data, 0, bi, 0, bi.length);
                journal.add(new DeleteLogAction(mp.page.pageNo, bi));
                mp.page.dirty = true;
                mp.page.empty = true;
                next = mp.getNextPage();
                if (next != -1)
                    mp = new MessagePage(ctx.cacheManager.fetchAndPin(next));
            } while (next != -1);
        } else {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", toString() + "/remove, indexEntry=" + indexEntry + ", ref is " + ref + ", HOLD message");
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/remove done, indexEntry=" + indexEntry);
        return ref;
    }

    public void incDeliveryCount(QueueIndexEntry indexEntry) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/incDeliveryCount, indexEntry=" + indexEntry);
        indexEntry.setDeliveryCount(indexEntry.getDeliveryCount() + 1);
        replace(indexEntry.getKey(), indexEntry);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/incDeliveryCount done, indexEntry=" + indexEntry);
    }

    public void unloadPages() throws Exception {
        super.unloadPages();
        while (mpList.getSize() > 0) {
            ctx.cacheManager.unpin(((Page) mpList.remove()).pageNo);
        }
        pos.unloadPages();
    }

    public String toString() {
        return "[QueueIndex, " + super.toString() + "]";
    }
}

