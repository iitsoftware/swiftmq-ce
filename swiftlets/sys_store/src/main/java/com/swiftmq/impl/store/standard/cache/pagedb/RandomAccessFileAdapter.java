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
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class RandomAccessFileAdapter implements FileAccess {
    protected StoreContext ctx;
    protected String path;
    protected PriorityQueue<Integer> freePages = new PriorityQueue<>();
    protected final AtomicInteger nFree = new AtomicInteger();
    protected RandomAccessFile file;
    protected String filename;
    protected volatile long fileLength;
    protected final AtomicInteger numberPages = new AtomicInteger();
    protected int initialPages = 0;
    protected byte[] emptyData = null;
    private boolean offline = false;
    private boolean freePoolEnabled = true;

    public RandomAccessFileAdapter(StoreContext ctx, String path, int initialPages, boolean offline, boolean freePoolEnabled) throws Exception {
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

    private void initialize(int nPages) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/initialize, nPages=" + nPages);
        if (numberPages.get() > 0)
            throwException("can't initialize - page store contains " + numberPages + " pages");
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
                file.setLength(length);
                fileLength = length;
            } catch (Exception e) {
                panic(e);
            }
        }
    }

    private void buildFreePageList() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/buildFreePageList...");
        for (int i = 0; i < numberPages.get(); i++) {
            Page p = loadPage(i);
            if (p.empty && i > 0) // never put the root index page into the free page list!
                addToFreePool(p.pageNo);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", this + "/buildFreePageList, freePageNumbers.size=" + freePages());
    }

    private void throwException(String msg) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/throwException: " + msg);
        throw new Exception(msg);
    }

    public void init() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/init ...");
        nFree.set(0);
        filename = path + File.separatorChar + PageDB.FILENAME;
        emptyData = PageDB.makeEmptyArray(PageSize.getCurrent());
        file = new RandomAccessFile(filename, "rw");
        fileLength = file.length();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/init, fileLength=" + fileLength);
        if (fileLength > 0) {
            numberPages.set((int) (fileLength / PageSize.getCurrent()));
            if (!offline)
                shrinkFile();
        } else
            initialize(initialPages);
        sync();
        if (freePoolEnabled)
            buildFreePageList();
        ctx.logSwiftlet.logInformation("sys$store", this + "/init done, pageSize=" + PageSize.getCurrent() + ", size=" + numberPages.get() + ", freePages=" + freePages());
    }

    public Page initPage(int pageNo) {
        Page p = new Page();
        p.pageNo = pageNo;
        p.empty = true;
        p.dirty = false;
        p.data = new byte[PageSize.getCurrent()];
        System.arraycopy(emptyData, 0, p.data, 0, emptyData.length);
        return p;
    }

    public int numberPages() {
        return numberPages.get();
    }

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

    public Page loadPage(int pageNo) throws Exception {
        Page p = initPage(pageNo);
        long seekPoint = (long) pageNo * (long) PageSize.getCurrent();
        file.seek(seekPoint);
        file.readFully(p.data);
        p.empty = p.data[0] == 1;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", this + "/loadPage, pageNo=" + pageNo + ", empty=" + p.empty);
        return p;
    }

    public Page createPage() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/create ...");
        Page p = null;
        int pn = getFirstFree();
        if (pn != -1) {
            p = initPage(pn);
        } else {
            p = initPage(numberPages.get());
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
            ensureLength(pageNo);
            for (int i = numberPages.get(); i <= pageNo; i++) {
                Page p = initPage(i);
                writePage(p);
                addToFreePool(pageNo);
            }
            numberPages.set(pageNo + 1);
            extended = true;
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", this + "/ensure, numberPages=" + numberPages.get());
        return extended;
    }

    public void freePage(Page page) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/free, page=" + page);
        if (page.pageNo > numberPages.get())
            throwException("free pageNo " + page.pageNo + " is out of range [0.." + (numberPages.get()) + "]");
        page.empty = true;
        writePage(page);
        if (freePoolEnabled)
            addToFreePool(page.pageNo);
    }

    public void writePage(Page p) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/writePage, page=" + p);
        try {
            if (p.empty)
                System.arraycopy(emptyData, 0, p.data, 0, emptyData.length);
            p.data[0] = (byte) (p.empty ? 1 : 0);
            file.seek((long) p.pageNo * (long) PageSize.getCurrent());
            file.write(p.data);
            p.dirty = false;
            long length = (long) (p.pageNo + 1) * (long) PageSize.getCurrent();
            if (length > fileLength)
                fileLength = length;
        } catch (Exception e) {
            panic(e);
        }
    }

    public void sync() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/sync...");
        file.getFD().sync();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/sync done.");
    }

    private void truncateToPage(int pageNo) throws Exception {
        try {
            long length = (long) (pageNo + 1) * (long) PageSize.getCurrent();
            file.setLength(length);
            fileLength = length;
        } catch (Exception e) {
            panic(e);
        }
    }

    private void shrinkFile() throws Exception {
        int shrinked = numberPages.get();
        ctx.logSwiftlet.logInformation("sys$store", this + "/shrinkFile, before: numberPages=" + numberPages);
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

    public void shrink() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/shrink...");
        shrinkFile();
        freePages.clear();
        nFree.set(0);
        buildFreePageList();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/shrink done");
    }

    public void deleteStore() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/deleteStore");
        freePages.clear();
        numberPages.set(0);
        file.setLength(0);
        fileLength = 0;
    }

    public void close() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/close");
        if (freePoolEnabled)
            freePages.clear();
        numberPages.set(0);
        fileLength = 0;
        file.close();
    }

    public void reset() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/reset");
        close();
        init();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/reset done");
    }

    public String toString() {
        return "RandomAccessFileAdapter, file=" + filename + ", offline=" + offline + ", freePoolEnabled=" + freePoolEnabled;
    }
}

