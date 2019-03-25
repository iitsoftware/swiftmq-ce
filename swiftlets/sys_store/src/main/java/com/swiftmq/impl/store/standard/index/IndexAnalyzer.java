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
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.store.StoreEntry;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public class IndexAnalyzer {
    StoreContext ctx = null;
    boolean isError = false;

    public IndexAnalyzer(StoreContext ctx) {
        this.ctx = ctx;
    }

    private void dumpPage(PrintWriter writer, IndexPage p, boolean collect) throws Exception {
        writer.println(p);
        for (Iterator iter = p.iterator(); iter.hasNext(); ) {
            IndexEntry entry = (IndexEntry) iter.next();
            if (entry.isValid()) {
                if (collect) {
                    List list = new ArrayList();
                    int pageNo = entry.getRootPageNo();
                    MessagePage mp = null;
                    do {
                        list.add(Integer.valueOf(pageNo));
                        mp = new MessagePage(ctx.cacheManager.fetchAndPin(pageNo));
                        ctx.cacheManager.unpin(pageNo);
                        pageNo = mp.getNextPage();
                    } while (mp.getNextPage() != -1);
                    writer.println("  " + entry + " MessagePages=" + list);
                } else
                    writer.println("  " + entry);
            } else
                writer.println("  <deleted entry>");
        }
        writer.println("\n\n");
        ctx.cacheManager.shrink();
    }

    public void buildReferences() throws Exception {
        ctx.referenceMap.clear();
        RootIndexPage indexPage = new RootIndexPage(ctx, 0);
        indexPage.load();
        if (indexPage.getPrevPage() == 0)
            indexPage.unload();
        else {
            IndexPage actPage = indexPage;
            boolean done = false;
            do {
                for (Iterator iter = actPage.iterator(); iter.hasNext(); ) {
                    IndexEntry entry = (IndexEntry) iter.next();
                    if (entry.isValid())
                        buildQueueIndexReferences(entry.getRootPageNo());
                }
                actPage.unload();
                done = actPage.getNextPage() == -1;
                if (!done) {
                    actPage = new RootIndexPage(ctx, actPage.getNextPage());
                    actPage.load();
                }
            } while (!done);
        }
        ctx.referenceMap.removeReferencesLessThan(1);
        ctx.referenceMap.dump();
    }

    public void buildQueueIndexReferences(int rootPageNo) throws Exception {
        QueueIndexPage indexPage = new QueueIndexPage(ctx, rootPageNo);
        indexPage.load();
        if (indexPage.getPrevPage() == 0)
            indexPage.unload();
        else {
            IndexPage actPage = indexPage;
            boolean done = false;
            do {
                for (Iterator iter = actPage.iterator(); iter.hasNext(); ) {
                    IndexEntry entry = (IndexEntry) iter.next();
                    if (entry.isValid()) {
                        MessagePageReference ref = ctx.referenceMap.getReference(Integer.valueOf(entry.getRootPageNo()), true);
                        ref.incRefCount();
                    }
                }
                actPage.unload();
                done = actPage.getNextPage() == -1;
                if (!done) {
                    actPage = new QueueIndexPage(ctx, actPage.getNextPage());
                    actPage.load();
                }
            } while (!done);
        }
    }

    public void analyzeRootIndex(PrintWriter writer) throws Exception {
        RootIndexPage indexPage = new RootIndexPage(ctx, 0);
        indexPage.load();
        if (indexPage.getPrevPage() == 0) {
            writer.println("rootindex is empty");
            indexPage.unload();
        } else {
            IndexPage actPage = indexPage;
            boolean done = false;
            do {
                dumpPage(writer, actPage, false);
                for (Iterator iter = actPage.iterator(); iter.hasNext(); ) {
                    IndexEntry entry = (IndexEntry) iter.next();
                    if (entry.isValid())
                        analyzeQueueIndex(new PrintWriter(new FileWriter(entry.getKey() + ".analyze"), true), entry.getRootPageNo());
                }
                actPage.unload();
                done = actPage.getNextPage() == -1;
                if (!done) {
                    actPage = new RootIndexPage(ctx, actPage.getNextPage());
                    actPage.load();
                }
            } while (!done);
        }
    }

    public void analyzeQueueIndex(PrintWriter writer, int rootPageNo) throws Exception {
        QueueIndexPage indexPage = new QueueIndexPage(ctx, rootPageNo);
        indexPage.load();
        if (indexPage.getPrevPage() == 0) {
            writer.println("queueindex is empty");
            indexPage.unload();
        } else {
            IndexPage actPage = indexPage;
            boolean done = false;
            do {
                dumpPage(writer, actPage, true);
                actPage.unload();
                done = actPage.getNextPage() == -1;
                if (!done) {
                    actPage = new QueueIndexPage(ctx, actPage.getNextPage());
                    actPage.load();
                }
            } while (!done);
        }
    }

    public void checkConsistency(boolean recover) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/checkConsistency...");
        PR rootPR = new PR();
        rootPR.add(Long.valueOf(0));
        try {
            RootIndexPage indexPage = new RootIndexPage(ctx, 0);
            indexPage.load();
            if (indexPage.getPrevPage() == 0) {
                try {
                    indexPage.unload();
                } catch (Exception e) {
                }
            } else {
                RootIndexPage actPage = indexPage;
                boolean done = false;
                do {
                    actPage.setJournal(new ArrayList());
                    for (Iterator iter = actPage.iterator(); iter.hasNext(); ) {
                        IndexEntry entry = (IndexEntry) iter.next();
                        if (entry.isValid()) {
                            System.out.println("    Checking Queue Store: " + entry.getKey());
                            try {
                                rootPR.addAll(checkQueueIndex((String) entry.getKey(), entry.getRootPageNo()));
                            } catch (Exception e) {
                                if (recover) {
                                    if (ctx.traceSpace.enabled)
                                        ctx.traceSpace.trace("sys$store", toString() + "/checkConsistency, entry=" + entry + ", exception=" + e);
                                    ctx.logSwiftlet.logError("sys$store", "Queue Store " + entry.getKey() + " is inconsistent - removed!");
                                    System.out.println("    Queue Store " + entry.getKey() + " is inconsistent - removed!");
                                    iter.remove();
                                    isError = true;
                                } else {
                                    if (ctx.traceSpace.enabled)
                                        ctx.traceSpace.trace("sys$store", toString() + "/checkConsistency, entry=" + entry + ", exception=" + e);
                                    ctx.logSwiftlet.logError("sys$store", "Queue Store " + entry.getKey() + " is inconsistent - cancel consistency check!");
                                    throw e;
                                }
                            }
                        }
                    }
                    boolean pageChanged = false;
                    if (actPage.getNumberValidEntries() == 0) {
                        if (actPage.getPage().pageNo == 0) // Root Page
                        {
                            actPage.getPage().dirty = true;
                            actPage.getPage().empty = false; // never delete the root
                            if (actPage.getNextPage() != -1) {
                                pageChanged = true;
                                moveNextToRoot(actPage);
                            }
                        } else {
                            actPage.getPage().dirty = true;
                            actPage.getPage().empty = true;
                            pageChanged = true;
                            rechainAfterDelete(actPage);
                        }
                    }
                    try {
                        actPage.unload();
                    } catch (Exception e) {
                    }
                    if (pageChanged) {
                        actPage = new RootIndexPage(ctx, 0);
                        actPage.load();
                    } else {
                        done = actPage.getNextPage() == -1;
                        if (!done) {
                            rootPR.add(Long.valueOf(actPage.getNextPage()));
                            actPage = new RootIndexPage(ctx, actPage.getNextPage());
                            actPage.load();
                        }
                    }
                } while (!done);
            }
            try {
                ctx.cacheManager.flush();
                ctx.cacheManager.reset();
            } catch (Exception e) {
            }
            removeUnrefPages(rootPR);
        } catch (Exception e) {
            if (recover) {
                System.out.println("    Unrecoverable Error -- have to delete the whole persistent Store!");
                ctx.logSwiftlet.logError("sys$store", "Unrecoverable Error -- have to delete the whole persistent Store!");
                isError = true;
                try {
                    ctx.stableStore.deleteStore();
                    ctx.cacheManager.reset();
                } catch (Exception e1) {
                }
            } else {
                System.out.println("    Unrecoverable Error -- persistent Store is inconsistent! Instance STOPPED!");
                ctx.logSwiftlet.logError("sys$store", "Unrecoverable Error -- persistent Store is inconsistent! Instance STOPPED!");
                SwiftletManager.getInstance().disableShutdownHook();
                System.exit(-1);
            }
        }
        if (isError) {
            ctx.logSwiftlet.logError("sys$store", "The consistency check of the persistent store has found inconsistent data.");
            ctx.logSwiftlet.logError("sys$store", "This has been corrected (the store is now consistent).");
            ctx.logSwiftlet.logError("sys$store", "However, you might have lost persistent data.");
            ctx.logSwiftlet.logError("sys$store", "To avoid inconsistent data in future,");
            ctx.logSwiftlet.logError("sys$store", "please consider to enable 'force-sync' of the transaction log.");
            System.out.println("\n*** The consistency check of the persistent store has found inconsistent data.");
            System.out.println("*** This has been corrected (the store is now consistent).");
            System.out.println("*** However, you might have lost persistent data.");
            System.out.println("*** To avoid inconsistent data in future,");
            System.out.println("*** please consider to enable 'force-sync' of the transaction log.\n");
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/checkConsistency...done.");
    }

    private void moveNextToRoot(RootIndexPage current) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/moveNextToRoot...");
        RootIndexPage next = new RootIndexPage(ctx, current.getNextPage());
        next.load();
        next.setJournal(new ArrayList());
        System.arraycopy(next.getPage().data, 0, current.getPage().data, 0, next.getFirstFreePosition());
        current.setPrevPage(-1);
        current.initValues();
        next.getPage().dirty = true;
        next.getPage().empty = true;
        next.unload();
        if (next.getNextPage() != -1) {
            next = new RootIndexPage(ctx, next.getNextPage());
            next.load();
            next.setJournal(new ArrayList());
            next.setPrevPage(current.getPage().pageNo);
            next.unload();
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/moveNextToRoot...done, current=" + current);
    }

    private void rechainAfterDelete(RootIndexPage current) throws Exception {
        System.out.println("*** rechainAfterDelete, current=" + current);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/rechainAfterDelete...");
        if (current.getPrevPage() != -1) {
            RootIndexPage prev = new RootIndexPage(ctx, current.getPrevPage());
            prev.load();
            prev.setJournal(new ArrayList());
            prev.setNextPage(current.getNextPage());
            prev.unload();
        }
        if (current.getNextPage() != -1) {
            RootIndexPage next = new RootIndexPage(ctx, current.getNextPage());
            next.load();
            next.setJournal(new ArrayList());
            next.setPrevPage(current.getPrevPage());
            next.unload();
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/rechainAfterDelete...done.");
    }

    private void removeUnrefPages(PR rootPR) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/removeUnrefPages...");
        try {
            ctx.stableStore.reset();
        } catch (Exception e) {
        }
        int max = ctx.stableStore.getNumberPages();
        for (int i = 0; i < max; i++) {
            try {
                Page p = ctx.stableStore.get(i);
                if (!p.empty && !rootPR.contains(Long.valueOf(i))) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace("sys$store", toString() + "/removeUnrefPages, pageNo " + i + " unreferenced -> freePage");
                    ctx.stableStore.free(p);
                }
            } catch (Exception e) {
            }
        }
        try {
            ctx.stableStore.reset();
        } catch (Exception e) {
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/removeUnrefPages...done.");
    }

    private PR checkQueueIndex(String queueName, int rootPageNo) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/checkQueueIndex, queueName=" + queueName + ", rootPageNo=" + rootPageNo + " ...");
        PR indexPR = new PR();
        indexPR.add(Long.valueOf(rootPageNo));
        QueueIndexPage indexPage = new QueueIndexPage(ctx, rootPageNo);
        indexPage.load();
        if (indexPage.getPrevPage() == 0) {
            try {
                indexPage.unload();
            } catch (Exception e) {
            }
        } else {
            QueueIndexPage actPage = indexPage;
            boolean done = false;
            do {
                indexPR.addAll(checkPage(actPage));
                try {
                    actPage.unload();
                } catch (Exception e) {
                }
                done = actPage.getNextPage() == -1;
                if (!done) {
                    indexPR.add(Long.valueOf(actPage.getNextPage()));
                    actPage = new QueueIndexPage(ctx, actPage.getNextPage());
                    actPage.load();
                }
            } while (!done);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/checkQueueIndex, queueName=" + queueName + ", rootPageNo=" + rootPageNo + " done");
        return indexPR;
    }

    private PR checkPage(QueueIndexPage p) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/checkPage, p=" + p + " ...");
        PR pagePR = new PR();
        List entries = new ArrayList();
        for (Iterator iter = p.iterator(); iter.hasNext(); ) {
            IndexEntry entry = (IndexEntry) iter.next();
            if (entry.isValid())
                entries.add(entry);
        }
        for (Iterator iter = entries.iterator(); iter.hasNext(); ) {
            QueueIndexEntry entry = (QueueIndexEntry) iter.next();
            try {
                PR elementPR = new PR();
                PageInputStream pis = new PageInputStream(ctx);
                pis.setPageRecorder(elementPR);
                pis.setRootPageNo(entry.getRootPageNo());
                StoreEntry storeEntry = new StoreEntry();
                storeEntry.key = entry;
                storeEntry.priority = entry.getPriority();
                storeEntry.deliveryCount = entry.getDeliveryCount();
                storeEntry.expirationTime = entry.getExpirationTime();
                storeEntry.message = MessageImpl.createInstance(pis.readInt());
                storeEntry.message.readContent(pis);
                pis.unloadPages();
                pis.reset();
                pagePR.addAll(elementPR);
            } catch (Exception e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$store", toString() + "/checkPage, p=" + p + ", entry=" + entry + ", exception=" + e);
                throw e;
            }
        }
        try {
            ctx.cacheManager.shrink();
        } catch (Exception e) {
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/checkPage, p=" + p + " done");
        return pagePR;
    }

    public String toString() {
        return "IndexAnalyzer";
    }

    private class PR extends HashSet implements PageRecorder {
        public void recordPageNo(long pageNo) {
            add(Long.valueOf(pageNo));
        }
    }
}
