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
import com.swiftmq.swiftlet.SwiftletManager;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class MemoryMappedFileAdapter implements FileAccess {
    protected StoreContext ctx;
    protected String path;
    protected PriorityQueue<Integer> freePages = new PriorityQueue<>();
    protected final AtomicInteger nFree = new AtomicInteger();
    protected RandomAccessFile file;
    protected FileChannel fileChannel;
    protected MappedByteBuffer mappedBuffer;
    protected String filename;
    protected volatile long fileLength;
    protected final AtomicInteger numberPages = new AtomicInteger();
    protected int initialPages = 0;
    protected byte[] emptyData = null;
    private boolean offline = false;
    private boolean freePoolEnabled = true;

    public MemoryMappedFileAdapter(StoreContext ctx, String path, int initialPages, boolean offline, boolean freePoolEnabled) throws Exception {
        this.ctx = ctx;
        this.path = path;
        this.initialPages = initialPages;
        this.offline = offline;
        this.freePoolEnabled = freePoolEnabled;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/creating ...");
        new File(this.path).mkdirs();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/creating done.");
    }

    private void panic(Exception e) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/panic: " + e);
        System.err.println("PANIC, EXITING: " + e);
        e.printStackTrace();
        SwiftletManager.getInstance().disableShutdownHook();
        System.exit(-1);
    }

    private void buildFreePageList() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/buildFreePageList...");
        freePages.clear();
        nFree.set(0);
        for (int i = 0; i < numberPages.get(); i++) {
            Page p = loadPage(i);
            if (p.empty && i > 0) // never put the root index page into the free page list!
                addToFreePool(p.pageNo);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", this + "/buildFreePageList, freePageNumbers.size=" + freePages());
    }

    private void addToFreePool(int pageNo) {
        if (!freePoolEnabled)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/addToFreePool, pageNo=" + pageNo);
        freePages.offer(pageNo);
        nFree.getAndIncrement();
    }

    private int getFirstFree() {
        if (!freePoolEnabled || nFree.get() == 0)
            return -1;
        int pageNo = freePages.poll();
        nFree.getAndDecrement();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/getFirstFree, pageNo=" + pageNo);
        return pageNo;
    }

    private void unmapFile() {
        if (mappedBuffer != null) {
            mappedBuffer.force(); // Save changes to the file
            mappedBuffer = null;
        }
    }

    private void mapFile(boolean buildFreePages) throws Exception {
        if (fileLength > 0) {
            mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLength);
            numberPages.set((int) (fileLength / PageSize.getCurrent()));
            if (buildFreePages && freePoolEnabled)
                buildFreePageList();
        }
    }

    private void initialize(int nPages) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/initialize, nPages=" + nPages);
        if (numberPages.get() > 0)
            throw new Exception("can't initialize - page store contains " + numberPages.get() + " pages");
        ensurePage(nPages - 1);
        for (int i = 0; i < nPages; i++) {
            Page p = initPage(i);
            writePage(p);
        }
        numberPages.set(nPages);
    }

    private void ensureLength(int pageNo) throws Exception {
        long length = (long) (pageNo + 1) * (long) PageSize.getCurrent();
        if (fileLength < length) {
            try {
                unmapFile();
                file.setLength(length);
                fileLength = length;
                mapFile(false);
            } catch (Exception e) {
                panic(e);
            }
        }
    }

    private void truncateToPage(int pageNo) throws Exception {
        try {
            unmapFile();
            long length = (long) (pageNo + 1) * (long) PageSize.getCurrent();
            file.setLength(length);
            fileLength = length;
            mapFile(true);
        } catch (Exception e) {
            panic(e);
        }
    }

    private void shrinkFile() throws Exception {
        int shrinked = numberPages.get();
        ctx.logSwiftlet.logInformation("sys$store", this + "/shrinkFile, before: numberPages=" + numberPages);
        if (mappedBuffer == null)
            mapFile(true);
        for (int i = numberPages.get() - 1; i >= initialPages; i--) {
            Page p = loadPage(i);
            if (p.empty)
                shrinked--;
            else
                break;
        }
        numberPages.set(shrinked);
        truncateToPage(numberPages.get() - 1);
        ctx.logSwiftlet.logInformation("sys$store", this + "/shrinkFile, after: numberPages=" + numberPages);
    }

    @Override
    public void init() throws Exception {
        filename = path + File.separatorChar + PageDB.FILENAME;
        file = new RandomAccessFile(filename, "rw");
        fileChannel = file.getChannel();
        fileLength = file.length();
        emptyData = PageDB.makeEmptyArray(PageSize.getCurrent());
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/init, fileLength=" + fileLength);
        if (fileLength > 0) {
            numberPages.set((int) (fileLength / PageSize.getCurrent()));
            if (!offline)
                shrinkFile();
        } else
            initialize(initialPages);
        sync();
        ctx.logSwiftlet.logInformation("sys$store", this + "/init done, pageSize=" + PageSize.getCurrent() + ", size=" + numberPages.get() + ", freePages=" + freePages());
    }

    // Implement FileAccess methods using MappedByteBuffer

    @Override
    public int numberPages() {
        return numberPages.get();
    }

    @Override
    public int freePages() {
        return nFree.get();
    }

    @Override
    public int usedPages() {
        return numberPages.get() - nFree.get();
    }

    @Override
    public long fileSize() {
        return fileLength;
    }

    @Override
    public Page initPage(int pageNo) {
        Page p = new Page();
        p.pageNo = pageNo;
        p.empty = true;
        p.dirty = false;
        p.data = new byte[PageSize.getCurrent()];
        System.arraycopy(emptyData, 0, p.data, 0, emptyData.length);
        return p;
    }

    @Override
    public Page loadPage(int pageNo) throws Exception {
        if (ctx.traceSpace.enabled) {
            ctx.traceSpace.trace("sys$store", this + "/loadPage, pageNo=" + pageNo);
        }

        int pageSize = PageSize.getCurrent();
        long offset = (long) pageNo * pageSize;
        if (offset + pageSize > fileLength) {
            throw new Exception("Page number " + pageNo + " out of file bounds");
        }

        Page p = new Page();
        p.pageNo = pageNo;
        p.data = new byte[pageSize];

        mappedBuffer.position((int) offset);
        mappedBuffer.get(p.data, 0, pageSize);

        p.empty = p.data[0] == 1;
        return p;
    }

    @Override
    public Page createPage() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/create ...");
        Page p = null;
        int pn = getFirstFree();
        if (pn != -1) {
            p = initPage(pn);
        } else {
            pn = numberPages.get();
            p = initPage(pn);
            if (!ensurePage(pn))
                numberPages.getAndIncrement();
            writePage(p);
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/create, page=" + p);
        return p;
    }

    public boolean ensurePage(int pageNo) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/ensure, pageNo=" + pageNo);
        boolean extended = false;
        if (pageNo > numberPages.get() - 1) {
            int extend = pageNo + 1000;
            ensureLength(extend);
            for (int i = numberPages.get(); i <= extend; i++) {
                Page p = initPage(i);
                writePage(p);
                addToFreePool(i);
            }
            numberPages.set(extend + 1);
            extended = true;
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", this + "/ensure, numberPages=" + numberPages.get());
        return extended;
    }

    public void freePage(Page page) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/free, page=" + page);
        if (page.pageNo > numberPages.get())
            throw new Exception("free pageNo " + page.pageNo + " is out of range [0.." + (numberPages.get()) + "]");
        page.empty = true;
        writePage(page);
        if (freePoolEnabled)
            addToFreePool(page.pageNo);
    }

    @Override
    public void writePage(Page p) throws Exception {
        if (ctx.traceSpace.enabled) {
            ctx.traceSpace.trace("sys$store", this + "/writePage, page=" + p);
        }

        int pageSize = PageSize.getCurrent();
        long offset = (long) p.pageNo * pageSize;
        if (offset + pageSize > fileLength) {
            throw new Exception("Page number " + p.pageNo + " out of file bounds");
        }

        mappedBuffer.position((int) offset);
        if (p.empty) {
            System.arraycopy(emptyData, 0, p.data, 0, emptyData.length);
        }
        p.data[0] = (byte) (p.empty ? 1 : 0);
        mappedBuffer.put(p.data, 0, pageSize);
        p.dirty = false;
    }

    @Override
    public void shrink() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/shrink...");
        shrinkFile();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/shrink done");
    }

    @Override
    public void sync() throws Exception {
        if (mappedBuffer != null) {
            mappedBuffer.force(); // Persist changes to disk
        }
    }

    public void close() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/close");
        unmapFile();
        numberPages.set(0);
        fileLength = 0;
        fileChannel.close();
        file.close();
    }

    public void reset() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/reset");
        close();
        init();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/reset done");
    }

    public void deleteStore() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/deleteStore");
        unmapFile();
        file.setLength(0);
        fileLength = 0;
    }

    public String toString() {
        return "MemoryMappedFileAdapter, file=" + filename + ", offline=" + offline + ", freePoolEnabled=" + freePoolEnabled;
    }
}
