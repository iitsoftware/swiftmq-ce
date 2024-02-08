/*
 * Copyright 2024 IIT Software GmbH
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

package com.swiftmq.impl.store.standard.cache.pagedb;

import com.swiftmq.impl.store.standard.StoreContext;
import com.swiftmq.impl.store.standard.cache.Page;
import com.swiftmq.impl.store.standard.pagedb.PageSize;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class MappedFileRegion {
    StoreContext ctx;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private MappedByteBuffer buffer;
    private final String filePath;
    private final int regionStartPage;
    private final int regionEndPage;
    private final int numberPagesInRegion;
    private final long regionSize;  // Size of the region in bytes
    private final int pageSize;
    private boolean dirty = false;
    private volatile boolean empty = true; // Only valid immediately after the scan; means only free pages in this region
    ReentrantLock lock = new ReentrantLock();

    public MappedFileRegion(StoreContext ctx, String filePath, int regionStartPage, long regionSize) throws IOException {
        this.ctx = ctx;
        this.filePath = filePath;
        this.regionStartPage = regionStartPage;
        this.pageSize = PageSize.getCurrent();
        this.regionSize = regionSize;
        this.regionEndPage = regionStartPage + (int) (regionSize / pageSize) - 1;
        this.numberPagesInRegion = regionEndPage - regionStartPage + 1;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/created");
        open();
    }

    private void open() throws IOException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/open ...");
        randomAccessFile = new RandomAccessFile(new File(filePath), "rw");
        fileChannel = randomAccessFile.getChannel();
        extendFileIfNeeded();
        buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, (long) regionStartPage * pageSize, regionSize);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/open done");
    }

    public List<Integer> scanForFreePages() {
        lock.lock();  // This must be using a lock to guarantee visibility according to the JMM (parallelStream usage)
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/scanForFreePages ...");
            List<Integer> freePageList = new ArrayList<>();
            for (int i = 0; i < numberPagesInRegion; i++) {
                buffer.position(i * pageSize);
                byte pageStatus = buffer.get();
                int pageNo = regionStartPage + i;
                if (pageStatus == 1 && pageNo != 0) { // 1 indicates the page is empty, don't add root index page 0
                    freePageList.add(pageNo);
                } else
                    empty = false;
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", this + "/scanForFreePages done, free=" + freePageList.size() + ", empty=" + empty);
            return freePageList;
        } finally {
            lock.unlock();
        }

    }

    private void extendFileIfNeeded() throws IOException {
        long currentLength = fileChannel.size();
        long requiredLength = (long) regionStartPage * pageSize + regionSize;
        if (currentLength < requiredLength) {
            // Extend and initialize new pages
            fileChannel.position(currentLength);
            long remaining = requiredLength - currentLength;
            byte[] emptyPageData = createEmptyPageData();

            while (remaining > 0) {
                fileChannel.write(java.nio.ByteBuffer.wrap(emptyPageData));
                remaining -= pageSize;
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", this + "/extendFileIfNeeded, currentLength=" + currentLength + ", requiredLength=" + requiredLength);
    }

    private byte[] createEmptyPageData() {
        byte[] data = new byte[pageSize];
        data[0] = 1; // Mark as empty
        return data;
    }

    private void checkPageBounds(int pageNo) throws IOException {
        if (pageNo < regionStartPage || pageNo > regionEndPage) {
            throw new IOException("Page number " + pageNo + " is out of region bounds");
        }
    }

    public boolean isEmpty() {
        return empty;
    }

    public int getRegionStartPage() {
        return regionStartPage;
    }

    public Page getPage(int pageNo) throws IOException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/getPage, pageNo=" + pageNo);
        checkPageBounds(pageNo);
        int offsetInRegion = (pageNo - regionStartPage) * pageSize;
        buffer.position(offsetInRegion);
        byte[] data = new byte[pageSize];
        buffer.get(data);
        Page page = new Page();
        page.pageNo = pageNo;
        page.data = data;
        page.empty = data[0] == 1;
        return page;
    }

    public void writePage(Page page) throws IOException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/writePage, pageNo=" + page.pageNo);
        checkPageBounds(page.pageNo);
        int offsetInRegion = (page.pageNo - regionStartPage) * pageSize;
        buffer.position(offsetInRegion);
        buffer.put(page.data, 0, pageSize);
        dirty = true;
    }

    public void freePage(int pageNo) throws IOException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/freePage, pageNo=" + pageNo);
        checkPageBounds(pageNo);
        Page emptyPage = new Page();
        emptyPage.pageNo = pageNo;
        emptyPage.data = createEmptyPageData();
        emptyPage.empty = true;
        writePage(emptyPage);
    }

    public void sync() {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/sync");
            if (dirty) {
                buffer.force();
                dirty = false;
            }
        } finally {
            lock.unlock();
        }

    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/close");
        try {
            sync();
            if (fileChannel != null) {
                fileChannel.close();
                fileChannel = null;
            }
            if (randomAccessFile != null) {
                randomAccessFile.close();
                randomAccessFile = null;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String toString() {
        return "MappedFileRegion, regionStartPage=" + regionStartPage + ", regionEndPage=" + regionEndPage + ", dirty=" + dirty;
    }
}
