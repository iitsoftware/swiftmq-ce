/*
 * Copyright 2022 IIT Software GmbH
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

package com.swiftmq.impl.store.standard.pagesize;

import com.swiftmq.impl.store.standard.StoreContext;
import com.swiftmq.impl.store.standard.cache.StableStore;
import com.swiftmq.impl.store.standard.index.*;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.File;
import java.util.*;

public class StoreConverter {
    private final String pagedbDir;
    private StoreContext ctx;
    private int currentSize = 0;
    private int recommendedSize = 0;
    private boolean converted = false;
    private DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();
    private Map<Integer, Integer> messageSizes = new TreeMap<>();

    public StoreConverter(StoreContext ctx, String pagedbDir) {
        this.ctx = ctx;
        this.pagedbDir = pagedbDir;
    }

    public void convertStore() throws Exception {
        String oldPageDB = pagedbDir + File.separatorChar + StableStore.FILENAME;
        String newPageDB = pagedbDir + File.separatorChar + StableStore.FILENAME + "_new";
        if (!new File(oldPageDB).exists())
            return;
        currentSize = PageSize.getCurrent();
        recommendedSize = PageSize.getRecommended();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/convert, current page size: " + currentSize + ", new page size: " + recommendedSize);
        if (recommendedSize < currentSize) {
            ctx.logSwiftlet.logInformation("sys$store", toString() + "/cannot convert store to a recommended page size (" + recommendedSize + ") that is < current size (" + currentSize + ")");
            return;
        }
        if (recommendedSize == currentSize) {
            ctx.logSwiftlet.logInformation("sys$store", toString() + "/nothing to convert, current page size is equal to recommended page size.");
            return;
        }
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/converting page.db from page size " + currentSize + " to " + recommendedSize);
        StableStore.copyToNewSize(oldPageDB, newPageDB, currentSize, recommendedSize);
        String oldPageDBsaved = oldPageDB + "_" + System.currentTimeMillis();
        new File(oldPageDB).renameTo(new File(oldPageDBsaved));
        new File(newPageDB).renameTo(new File(oldPageDB));
        PageSize.setCurrent(recommendedSize);
        converted = true;
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/converting page.db done. The old page.db can be found under " + oldPageDBsaved);
    }

    private void countMessageSizes(QueueIndexEntry queueIndexEntry) throws Exception {
        long size = 0;
        MessagePage messagePage = new MessagePage(ctx.cacheManager.fetchAndPin(queueIndexEntry.getRootPageNo()));
        size += messagePage.getLength() - MessagePage.START_DATA;
        ctx.cacheManager.unpin(messagePage.page.pageNo);
        while (messagePage.getNextPage() != -1) {
            messagePage = new MessagePage(ctx.cacheManager.fetchAndPin(messagePage.getNextPage()));
            size += messagePage.getLength() - MessagePage.START_DATA;
            ctx.cacheManager.unpin(messagePage.page.pageNo);
        }
        int sizeInKB = (int) Math.round(((double) (size + 512) / 1024.0));
        messageSizes.putIfAbsent(sizeInKB, 0);
        messageSizes.put(sizeInKB, messageSizes.get(sizeInKB) + 1);
    }

    private void rebuildMessagePages(QueueIndexEntry queueIndexEntry) throws Exception {
        dbos.rewind();
        List<MessagePage> list = new ArrayList<>();
        MessagePage messagePage = new MessagePage(ctx.cacheManager.fetchAndPin(queueIndexEntry.getRootPageNo()));
        messagePage.writeData(dbos);
        list.add(messagePage);
        while (messagePage.getNextPage() != -1) {
            messagePage = new MessagePage(ctx.cacheManager.fetchAndPin(messagePage.getNextPage()));
            messagePage.writeData(dbos);
            list.add(messagePage);
        }
        int nPages = list.size();
        int used = 0;
        int released = 0;
        byte[] buffer = dbos.getBuffer();
        int bufferLen = dbos.getCount();
        int bufferPos = 0;
        while (bufferPos < bufferLen) {
            messagePage = list.remove(0);
            int len = Math.min(PageSize.getCurrent() - MessagePage.START_DATA, bufferLen - bufferPos);
            System.arraycopy(buffer, bufferPos, messagePage.page.data, MessagePage.START_DATA, len);
            bufferPos += len;
            if (bufferPos == bufferLen) {
                messagePage.setNextPage(-1);
            }
            messagePage.setLength(len + MessagePage.START_DATA);
            messagePage.page.dirty = true;
            messagePage.page.empty = false;
            ctx.cacheManager.unpin(messagePage.page.pageNo);
            used++;
        }
        for (MessagePage m : list) {
            m.page.empty = true;
            m.page.dirty = true;
            ctx.cacheManager.unpin(m.page.pageNo);
            released++;
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/rebuildMessagePages, nPages=" + nPages + ", used=" + used + ", released=" + released);
    }

    private void iterateQueueIndexPage(QueueIndexPage queueIndexPage, QIEProcessor processor) throws Exception {
        for (Iterator iter = queueIndexPage.iterator(); iter.hasNext(); ) {
            QueueIndexEntry entry = (QueueIndexEntry) iter.next();
            if (entry.isValid())
                processor.process(entry);
        }
        ctx.cacheManager.shrink();
    }

    private void iterateQueueIndex(String queueName, int rootPageNo, QIEProcessor processor) throws Exception {
        QueueIndexPage actPage = new QueueIndexPage(ctx, rootPageNo);
        actPage.load();
        if (actPage.getPrevPage() == 0) {
            actPage.unload();
        } else {
            boolean done = false;
            do {
                iterateQueueIndexPage(actPage, processor);
                actPage.unload();
                done = actPage.getNextPage() == -1;
                if (!done) {
                    actPage = new QueueIndexPage(ctx, actPage.getNextPage());
                    actPage.load();
                }
            } while (!done);
        }
    }

    public void iterateRootIndex(QIEProcessor processor) throws Exception {
        RootIndexPage actPage = new RootIndexPage(ctx, 0);
        actPage.load();
        if (actPage.getPrevPage() == 0) {
            actPage.unload();
        } else {
            boolean done = false;
            do {
                for (Iterator<IndexEntry> iter = actPage.iterator(); iter.hasNext(); ) {
                    IndexEntry entry = iter.next();
                    if (entry.isValid())
                        iterateQueueIndex((String) entry.getKey(), entry.getRootPageNo(), processor);
                }
                actPage.unload();
                done = actPage.getNextPage() == -1;
                if (!done) {
                    actPage = new RootIndexPage(ctx, actPage.getNextPage());
                    actPage.load();
                }
            } while (!done);
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/iterateRootIndex done");
    }

    public void convertMessagePages() throws Exception {
        if (!converted)
            return;
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/convertMessagePages ...");
        iterateRootIndex(this::rebuildMessagePages);
        ctx.cacheManager.flush();
        ctx.cacheManager.reset();
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/convertMessagePages done");
    }

    public void determineRecommendedPageSize() throws Exception {
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/determineRecommendedPageSize ...");
        messageSizes.clear();
        iterateRootIndex(this::countMessageSizes);
        long totalSizeInKB = 0;
        long totalMsgs = 0;
        for (Map.Entry<Integer, Integer> entry : messageSizes.entrySet()) {
            Integer sizeInKB = entry.getKey();
            Integer count = entry.getValue();
            ctx.logSwiftlet.logInformation("sys$store", toString() + "/determineRecommendedPageSize/--- Message Size in KB: " + sizeInKB + ", Number Messages: " + count);
            totalSizeInKB += (long) sizeInKB * (long) count;
            totalMsgs += count;
        }
        int recommended = Math.min(Math.max((int) ((((totalSizeInKB * 1024) / totalMsgs / 2048) + 1) * 2048), 2048), PageSize.maxPageSize());
        PageSize.setRecommended(recommended);
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/determineRecommendedPageSize/*** Current Page Size: " + PageSize.getCurrent() + " / Recommended Page Size: " + recommended);
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/determineRecommendedPageSize done");
    }

    public String toString() {
        return "StoreConverter";
    }

    private interface QIEProcessor {
        void process(QueueIndexEntry queueIndexEntry) throws Exception;
    }
}
