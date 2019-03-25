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
import com.swiftmq.impl.store.standard.cache.CacheReleaseListener;
import com.swiftmq.impl.store.standard.log.CommitLogRecord;
import com.swiftmq.impl.store.standard.log.DeleteLogAction;
import com.swiftmq.impl.store.standard.log.InsertLogAction;
import com.swiftmq.impl.store.standard.log.UpdatePortionLogAction;
import com.swiftmq.tools.concurrent.Semaphore;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RootIndex extends Index {
    public RootIndex(StoreContext ctx, int rootPageNo) throws Exception {
        super(ctx, rootPageNo);
        RootIndexPage indexPage = new RootIndexPage(ctx, rootPageNo);
        indexPage.load();
        if (indexPage.getPrevPage() == 0) {
            Semaphore sem = new Semaphore();
            List journal = new ArrayList();
            journal.add(new InsertLogAction(indexPage.getPage().pageNo));
            indexPage.setJournal(journal);
            indexPage.setPrevPage(-1);
            indexPage.setNextPage(-1);
            long txId = ctx.transactionManager.createTxId();
            CommitLogRecord logRecord = new CommitLogRecord(txId, sem, journal, new IndexPageRelease(this, indexPage), null);
            ctx.recoveryManager.commit(logRecord);
            sem.waitHere();
            ctx.transactionManager.removeTxId(txId);
        } else
            indexPage.unload();
    }

    public IndexPage createIndexPage(int pageNo) throws Exception {
        return new RootIndexPage(ctx, pageNo);
    }

    private void shrinkPage(IndexPage current) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/shrinkPage..., current=" + current);
        byte[] beforeImage = new byte[current.getFirstFreePosition()];
        System.arraycopy(current.getPage().data, 0, beforeImage, 0, beforeImage.length);
        List list = new ArrayList();
        for (Iterator iter = current.iterator(); iter.hasNext(); ) {
            IndexEntry entry = (IndexEntry) iter.next();
            if (entry.isValid()) {
                list.add(entry);
                iter.remove();
            }
        }
        for (int i = 0; i < list.size(); i++) {
            IndexEntry entry = (IndexEntry) list.get(i);
            entry.setValid(true);
            current.addEntry(entry);
        }
        byte[] afterImage = new byte[current.getFirstFreePosition()];
        System.arraycopy(current.getPage().data, 0, afterImage, 0, afterImage.length);
        journal.add(new UpdatePortionLogAction(current.getPage().pageNo, 0, beforeImage, afterImage));
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/shrinkPage...done, current=" + current);
    }

    private IndexPage splitPage(IndexPage current, Comparable key) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/splitPage, before, current: " + current);
        IndexPage newPage = getIndexPage(-1);
        newPage.setPrevPage(current.getPage().pageNo);
        newPage.setNextPage(current.getNextPage());
        if (current.getNextPage() != -1) {
            IndexPage next = getIndexPage(current.getNextPage());
            next.setPrevPage(newPage.getPage().pageNo);
        }
        current.setNextPage(newPage.getPage().pageNo);
        int nSplit = current.getNumberEntries() / 2;
        int n = 0;
        for (Iterator iter = current.iterator(); iter.hasNext(); ) {
            IndexEntry entry = (IndexEntry) iter.next();
            n++;
            if (n > nSplit) {
                newPage.addEntry(entry);
                iter.remove();
            }
        }
        shrinkPage(current);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/splitPage, after, current: " + current);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/splitPage, after, newPage: " + newPage);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/splitPage...done.");
        return current.getMaxKey().compareTo(key) > 0 ? current : newPage;
    }

    /**
     * @param entry
     */
    protected void add(IndexEntry entry) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/add, entry=" + entry + "...");
        if (journal == null)
            throw new NullPointerException("journal is null!");
        IndexPage current = getIndexPage(rootPageNo);
        while (!(current.getNextPage() == -1 || current.getNumberValidEntries() == 0 || current.getMaxKey().compareTo(entry.getKey()) > 0)) {
            current = getIndexPage(current.getNextPage());
        }
        if (current.available() < entry.getLength()) {
            if (current.getMaxKey().compareTo(entry.getKey()) < 0) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$store", toString() + "/add, entry=" + entry + ", create new page");
                IndexPage nextPage = getIndexPage(-1);
                current.setNextPage(nextPage.getPage().pageNo);
                nextPage.setPrevPage(current.getPage().pageNo);
                nextPage.setNextPage(-1);
                current = nextPage;
            } else {
                if (current.getNumberEntries() > current.getNumberValidEntries()) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace("sys$store", toString() + "/add, entry=" + entry + ", there is space, so shrink it");
                    shrinkPage(current);
                }
                if (current.available() < entry.getLength()) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace("sys$store", toString() + "/add, entry=" + entry + ", available=" + current.available() + ", entry.getLength()=" + entry.getLength() + ", split!");
                    current = splitPage(current, entry.getKey());
                }
            }
        }
        current.addEntry(entry);
        maxPage = -1;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/add, entry=" + entry + "...done.");
    }

    public synchronized QueueIndex getQueueIndex(String queueName) throws Exception {
        IndexEntry entry = find(queueName);
        if (entry == null) {
            Semaphore sem = new Semaphore();
            List journal = new ArrayList();
            setJournal(journal);
            QueueIndexPage indexPage = new QueueIndexPage(ctx, -1);
            journal.add(new InsertLogAction(indexPage.getPage().pageNo));
            indexPage.setJournal(journal);
            indexPage.setPrevPage(-1);
            indexPage.setNextPage(-1);
            entry = new RootIndexEntry();
            entry.setKey(queueName);
            entry.setRootPageNo(indexPage.getPage().pageNo);
            add(entry);
            long txId = ctx.transactionManager.createTxId();
            CommitLogRecord logRecord = new CommitLogRecord(txId, sem, journal, new IndexPageRelease(this, indexPage), null);
            ctx.recoveryManager.commit(logRecord);
            sem.waitHere();
            ctx.transactionManager.removeTxId(txId);
        } else
            unloadPages();
        return new QueueIndex(ctx, entry.getRootPageNo());
    }

    public synchronized void deleteQueueIndex(String queueName, QueueIndex queueIndex) throws Exception {
        List entries = queueIndex.getEntries();
        Semaphore sem = new Semaphore();
        List journal = new ArrayList();
        queueIndex.setJournal(journal);
        List refs = new ArrayList();
        for (Iterator iter = entries.iterator(); iter.hasNext(); ) {
            MessagePageReference ref = queueIndex.remove((QueueIndexEntry) iter.next());
            if (ref != null)
                refs.add(ref);
            if (journal.size() > 10000) {
                long txId = ctx.transactionManager.createTxId();
                CommitLogRecord logRecord = new CommitLogRecord(txId, sem, journal, new QueueIndexRelease(queueIndex), refs);
                ctx.recoveryManager.commit(logRecord);
                sem.waitHere();
                sem.reset();
                journal.clear();
                refs = new ArrayList();
                ctx.transactionManager.removeTxId(txId);
            }
        }
        IndexPage root = queueIndex.getIndexPage(queueIndex.getRootPageNo());
        byte[] bi = new byte[root.getFirstFreePosition()];
        System.arraycopy(root.getPage().data, 0, bi, 0, bi.length);
        journal.add(new DeleteLogAction(root.getPage().pageNo, bi));
        root.getPage().dirty = true;
        root.getPage().empty = true;
        setJournal(journal);
        remove(queueName);
        long txId = ctx.transactionManager.createTxId();
        CommitLogRecord logRecord = new CommitLogRecord(txId, sem, journal, new QueueRootIndexRelease(this, queueIndex), refs);
        ctx.recoveryManager.commit(logRecord);
        sem.waitHere();
        ctx.transactionManager.removeTxId(txId);
    }

    public String toString() {
        return "[RootIndex, " + super.toString() + "]";
    }

    private class IndexPageRelease implements CacheReleaseListener {
        RootIndex rootIndex = null;
        IndexPage indexPage = null;

        public IndexPageRelease(RootIndex rootIndex, IndexPage indexPage) {
            this.rootIndex = rootIndex;
            this.indexPage = indexPage;
        }

        public synchronized void releaseCache() {
            try {
                indexPage.unload();
                rootIndex.unloadPages();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private class QueueIndexRelease implements CacheReleaseListener {
        QueueIndex queueIndex = null;

        public QueueIndexRelease(QueueIndex queueIndex) {
            this.queueIndex = queueIndex;
        }

        public synchronized void releaseCache() {
            try {
                queueIndex.unloadPages();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private class QueueRootIndexRelease implements CacheReleaseListener {
        RootIndex rootIndex = null;
        QueueIndex queueIndex = null;

        public QueueRootIndexRelease(RootIndex rootIndex, QueueIndex queueIndex) {
            this.rootIndex = rootIndex;
            this.queueIndex = queueIndex;
        }

        public synchronized void releaseCache() {
            try {
                queueIndex.unloadPages();
                rootIndex.unloadPages();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

