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

package com.swiftmq.impl.store.standard.pagedb;

import com.swiftmq.impl.store.standard.StoreContext;
import com.swiftmq.impl.store.standard.cache.StableStore;
import com.swiftmq.impl.store.standard.index.*;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.swiftlet.store.StoreEntry;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.File;
import java.io.RandomAccessFile;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class StoreConverter {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd:HH:mm:ss");
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("0000000");
    private final String pagedbDir;
    private String pageDB;
    private String pageDBOld;
    private String pageDBNew;
    private StoreContext ctx;
    private int currentSize = 0;
    private int recommendedSize = 0;
    private boolean converted = false;
    private DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();
    private Map<Integer, Integer> messageSizes = new TreeMap<>();
    private Map<String, Integer> spoolQueues = new TreeMap<>();
    private String currentSpoolQueue = null;
    private RandomAccessFile currentSpoolFile = null;
    private int currentNumberMessages = 0;

    public StoreConverter(StoreContext ctx, String pagedbDir) {
        this.ctx = ctx;
        this.pagedbDir = pagedbDir;
        currentSize = PageSize.getCurrent();
        recommendedSize = PageSize.getRecommended();
        pageDB = pagedbDir + File.separatorChar + StableStore.FILENAME;
        pageDBOld = pagedbDir + File.separatorChar + StableStore.FILENAME + "_old";
        pageDBNew = pagedbDir + File.separatorChar + StableStore.FILENAME + "_new";
    }

    public boolean isConverted() {
        return converted;
    }

    public void scanPageDB() throws Exception {
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/scanPageDB ...");
        messageSizes.clear();
        iterateRootIndex(this::countMessageSizes);
        long totalSizeInKB = 0;
        long totalMsgs = 0;
        String[] names = ctx.scanList.getEntityNames();
        if (names != null) {
            for (String name : names) {
                ctx.scanList.removeEntity(ctx.scanList.getEntity(name));
            }
        }
        String dateString = DATE_FORMAT.format(new Date());
        for (Map.Entry<Integer, Integer> entry : messageSizes.entrySet()) {
            Integer sizeInKB = entry.getKey();
            Integer count = entry.getValue();
            ctx.logSwiftlet.logInformation("sys$store", toString() + "/scanPageDB/--- Message Size in KB: " + sizeInKB + ", Number Messages: " + count);
            Entity entity = ctx.scanList.createEntity();
            entity.setName(dateString + "-" + DECIMAL_FORMAT.format(sizeInKB));
            entity.createCommands();
            ctx.scanList.addEntity(entity);
            entity.getProperty("size").setValue(sizeInKB);
            entity.getProperty("number-messages").setValue(count);
            totalSizeInKB += (long) sizeInKB * (long) count;
            totalMsgs += count;
        }
        int recommended = Math.min(Math.max((int) ((((totalSizeInKB * 1024) / totalMsgs / 2048) + 1) * 2048), 2048), PageSize.maxPageSize());
        PageSize.setRecommended(recommended);
        messageSizes.clear();
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/scanPageDB/*** Current Page Size: " + PageSize.getCurrent() + " / Recommended Page Size: " + recommended);
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/scanPageDB done");
    }

    private boolean isDiskSpaceSufficient() {
        boolean rc = true;
        File currentFile = new File(pageDB);
        if (currentFile.exists()) {
            long nPages = currentFile.length() / currentSize;
            long required;
            if (currentSize < recommendedSize)
                required = nPages * recommendedSize + currentFile.length() + recommendedSize * 100L;
            else
                required = currentFile.length() * 3 + recommendedSize * 100L;
            File partition = new File(pagedbDir);
            long free = partition.getFreeSpace();
            ctx.logSwiftlet.logInformation("sys$store", toString() + "/disk space on " + pagedbDir);
            ctx.logSwiftlet.logInformation("sys$store", toString() + "/free: " + (free / (1024 * 1024)) + " MB,  required: " + (required / (1024 * 1024)) + " MB");
            if (free > required)
                ctx.logSwiftlet.logInformation("sys$store", toString() + "/disk space is sufficient");
            else {
                ctx.logSwiftlet.logInformation("sys$store", toString() + "/not enough disk space to convert the store!");
                rc = false;
            }
        }
        return rc;
    }

    public void phaseOne() throws Exception {
        if (!PageSize.isResizeOnStartup())
            return;
        if (!isDiskSpaceSufficient())
            return;
        if (currentSize == recommendedSize)
            ctx.logSwiftlet.logInformation("sys$store", toString() + "/Compacting page.db ...");
        else
            ctx.logSwiftlet.logInformation("sys$store", toString() + "/Converting page.db from page size " + currentSize + " to " + recommendedSize + " ...");
        File old = new File(pageDBOld);
        if (old.exists())
            old.delete();
        if (currentSize < recommendedSize)
            convertToLargePageSize();
        else
            spoolOutOldStore();
        PageSize.setCurrent(recommendedSize);
        ctx.dbEntity.getProperty("page-size-current").setValue(recommendedSize);
        converted = true;
    }

    public void phaseTwo() throws Exception {
        if (converted) {
            if (currentSize < recommendedSize)
                convertMessagePages();
            else
                spoolInNewStore();
            if (currentSize == recommendedSize)
                ctx.logSwiftlet.logInformation("sys$store", toString() + "/Compacting page.db done. The old page.db can be found under " + pageDBOld);
            else
                ctx.logSwiftlet.logInformation("sys$store", toString() + "/Converting page.db done. The old page.db can be found under " + pageDBOld);
        }
    }

    private void spoolOutOldStore() throws Exception {
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/spoolOutOldStore ...");
        deleteSpoolFiles();
        ctx.storeSwiftlet.startStore();
        iterateRootIndex(this::spoolOutMessage);
        ctx.cacheManager.flush();
        ctx.storeSwiftlet.stopStore();
        if (currentSpoolFile != null)
            currentSpoolFile.close();
        if (currentSpoolQueue != null)
            spoolQueues.put(currentSpoolQueue, currentNumberMessages);
        currentSpoolQueue = null;
        currentNumberMessages = 0;
        new File(pageDB).renameTo(new File(pageDBOld));
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/spoolOutOldStore done");
    }

    private void spoolInNewStore() throws Exception {
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/spoolInNewStore ...");
        RootIndex rootIndex = new RootIndex(ctx, 0);
        List journal = new ArrayList();
        rootIndex.setJournal(journal);
        for (Map.Entry<String, Integer> entry : spoolQueues.entrySet()) {
            String queueName = entry.getKey();
            int nMessages = entry.getValue();
            ctx.logSwiftlet.logInformation("sys$store", toString() + "/spoolInNewStore queueName=" + queueName + ", nMessages=" + nMessages);
            ensureSpoolFile(queueName);
            QueueIndex queueIndex = rootIndex.getQueueIndex(queueName);
            queueIndex.setJournal(journal);
            byte[] b = new byte[16];
            for (int i = 0; i < nMessages; i++) {
                currentSpoolFile.readFully(b);
                StoreEntry storeEntry = new StoreEntry();
                storeEntry.priority = Util.readInt(b, 0);
                storeEntry.deliveryCount = Util.readInt(b, 4);
                storeEntry.expirationTime = Util.readLong(b, 8);
                storeEntry.message = MessageImpl.createInstance(currentSpoolFile.readInt());
                storeEntry.message.readContent(currentSpoolFile);
                queueIndex.add(storeEntry);
                queueIndex.unloadPages();
                if (i % 1000 == 0)
                    ctx.cacheManager.flush();
            }
            ctx.cacheManager.flush();
        }
        ctx.cacheManager.reset();
        deleteSpoolFiles();
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/spoolInNewStore done");
    }

    private void deleteSpoolFiles() throws Exception {
        File[] files = new File(pagedbDir).listFiles((dir, name) -> name.endsWith(".spool"));
        if (files != null) {
            for (File f : files)
                f.delete();
        }
    }

    private void ensureSpoolFile(String queueName) throws Exception {
        if (currentSpoolQueue == null || !currentSpoolQueue.equals(queueName)) {
            if (currentSpoolFile != null)
                currentSpoolFile.close();
            if (currentSpoolQueue != null) {
                spoolQueues.put(currentSpoolQueue, currentNumberMessages);
                currentNumberMessages = 0;
            }
            currentSpoolFile = new RandomAccessFile(pagedbDir + File.separatorChar + queueName + ".spool", "rw");
            currentSpoolFile.seek(0);
            currentSpoolQueue = queueName;
        }
    }

    private void spoolOutMessage(RootIndex rootIndex, String queueName, QueueIndexEntry queueIndexEntry) throws Exception {
        ensureSpoolFile(queueName);
        QueueIndex queueIndex = rootIndex.getQueueIndex(queueName);
        StoreEntry storeEntry = queueIndex.get(queueIndexEntry);
        byte[] b = new byte[16];
        Util.writeInt(storeEntry.priority, b, 0);
        Util.writeInt(storeEntry.deliveryCount, b, 4);
        Util.writeLong(storeEntry.expirationTime, b, 8);
        currentSpoolFile.write(b);
        storeEntry.message.writeContent(currentSpoolFile);
        currentNumberMessages++;
    }

    private void convertToLargePageSize() throws Exception {
        if (!new File(pageDB).exists())
            return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/convert, current page size: " + currentSize + ", new page size: " + recommendedSize);
        StableStore.copyToNewSize(pageDB, pageDBNew, currentSize, recommendedSize);
        new File(pageDB).renameTo(new File(pageDBOld));
        new File(pageDBNew).renameTo(new File(pageDB));
    }

    private void countMessageSizes(RootIndex rootIndex, String queueName, QueueIndexEntry queueIndexEntry) throws Exception {
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

    private void rebuildMessagePages(RootIndex rootIndex, String queueName, QueueIndexEntry queueIndexEntry) throws Exception {
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

    private void iterateQueueIndexPage(RootIndex rootIndex, String queueName, QueueIndexPage queueIndexPage, QIEProcessor processor) throws Exception {
        for (Iterator iter = queueIndexPage.iterator(); iter.hasNext(); ) {
            QueueIndexEntry entry = (QueueIndexEntry) iter.next();
            if (entry.isValid())
                processor.process(rootIndex, queueName, entry);
        }
        ctx.cacheManager.shrink();
    }

    private void iterateQueueIndex(RootIndex rootIndex, String queueName, int rootPageNo, QIEProcessor processor) throws Exception {
        QueueIndexPage actPage = new QueueIndexPage(ctx, rootPageNo);
        actPage.load();
        if (actPage.getPrevPage() == 0) {
            actPage.unload();
        } else {
            boolean done = false;
            do {
                iterateQueueIndexPage(rootIndex, queueName, actPage, processor);
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
        RootIndex rootIndex = new RootIndex(ctx, 0);
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
                        iterateQueueIndex(rootIndex, (String) entry.getKey(), entry.getRootPageNo(), processor);
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

    private void convertMessagePages() throws Exception {
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/convertMessagePages ...");
        iterateRootIndex(this::rebuildMessagePages);
        ctx.cacheManager.flush();
        ctx.cacheManager.reset();
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/convertMessagePages done");
    }

    public String toString() {
        return "StoreConverter";
    }

    private interface QIEProcessor {
        void process(RootIndex rootIndex, String queueName, QueueIndexEntry queueIndexEntry) throws Exception;
    }
}
