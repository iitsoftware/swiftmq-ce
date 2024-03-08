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
import com.swiftmq.impl.store.standard.pagedb.PageSize;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.Property;
import com.swiftmq.mgmt.PropertyChangeListener;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.event.SwiftletManagerAdapter;
import com.swiftmq.swiftlet.event.SwiftletManagerEvent;
import com.swiftmq.swiftlet.mgmt.MgmtSwiftlet;
import com.swiftmq.swiftlet.mgmt.event.MgmtListener;
import com.swiftmq.swiftlet.timer.event.TimerListener;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class StableStore implements TimerListener, MgmtListener {
    public static final String FILENAME = "page.db";
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
    private Property sizeCollectProp = null;
    private SwiftletManagerAdapter swiftletManagerAdapter = null;
    private Property freeProp = null;
    private Property usedProp = null;
    private Property fileSizeProp = null;
    private volatile long collectInterval = 0;
    private volatile boolean mgmtToolAttached = false;
    private volatile boolean timerRunning = false;
    private MgmtSwiftlet mgmtSwiftlet = null;
    private boolean offline = false;
    private boolean freePoolEnabled = true;
    private volatile boolean checkPointActive = false;
    private long position = 0;

    public StableStore(StoreContext ctx, String path, int initialPages) throws Exception {
        this(ctx, path, initialPages, false, true);
    }

    public StableStore(StoreContext ctx, String path, int initialPages, boolean offline) throws Exception {
        this(ctx, path, initialPages, offline, true);
    }

    public StableStore(StoreContext ctx, String path, int initialPages, boolean offline, boolean freePoolEnabled) throws Exception {
        this.ctx = ctx;
        this.path = path;
        this.initialPages = initialPages;
        this.offline = offline;
        this.freePoolEnabled = freePoolEnabled;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/creating ...");
        new File(this.path).mkdirs();
        init();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/creating done.");
    }

    public int getnFree() {
        return nFree.get();
    }

    private static byte[] makeEmptyArray(int size) {
        byte[] data = new byte[size];
        data[0] = 1; // empty
        return data;
    }

    private void position(long seekPoint, int advance) throws Exception {
        if (position != seekPoint)
            file.seek(seekPoint);
        position = seekPoint + advance;
    }

    private static Page loadPage(RandomAccessFile file, int pageNo, int size) throws Exception {
        Page p = new Page();
        p.pageNo = pageNo;
        p.empty = true;
        p.dirty = false;
        p.data = new byte[size];
        long seekPoint = (long) pageNo * (long) size;
        file.seek(seekPoint);
        file.readFully(p.data);
        p.empty = p.data[0] == 1;
        return p;
    }

    private void startTimer() {
        if (mgmtToolAttached && collectInterval > 0) {
            ctx.timerSwiftlet.addTimerListener(collectInterval, StableStore.this);
            timerRunning = true;
        }
    }

    private void stopTimer() {
        if (timerRunning) {
            ctx.timerSwiftlet.removeTimerListener(StableStore.this);
            timerRunning = false;
        }
    }

    public boolean isCheckPointActive() {
        return checkPointActive;
    }

    public void setCheckPointActive(boolean checkPointActive) {
        this.checkPointActive = checkPointActive;
    }

    public void setFreePoolEnabled(boolean freePoolEnabled) {
        this.freePoolEnabled = freePoolEnabled;
    }

    public void adminToolActivated() {
        mgmtToolAttached = true;
        startTimer();
    }

    public void adminToolDeactivated() {
        mgmtToolAttached = false;
        stopTimer();
    }

    public void performTimeAction() {
        try {
            freeProp.setValue(nFree.get());
            usedProp.setValue(numberPages.get() - nFree.get());
            fileSizeProp.setValue(fileLength / 1024);
        } catch (Exception ignored) {
        }
    }

    private static void writePage(RandomAccessFile f, Page page, int newSize, byte[] empty) throws Exception {
        Page p = page.copy(newSize);
        if (p.empty)
            System.arraycopy(empty, 0, p.data, 0, empty.length);
        p.data[0] = (byte) (p.empty ? 1 : 0);
        f.seek((long) p.pageNo * (long) newSize);
        f.write(p.data);
        p.dirty = false;
    }

    public static void copyToNewSize(String sourceFilename, String destFilename, int oldSize, int newSize) throws Exception {
        RandomAccessFile sourceFile = new RandomAccessFile(sourceFilename, "r");
        int nPages = (int) (sourceFile.length() / oldSize);
        byte[] empty = makeEmptyArray(newSize);
        RandomAccessFile destFile = new RandomAccessFile(destFilename, "rw");
        for (int i = 0; i < nPages; i++) {
            writePage(destFile, loadPage(sourceFile, i, oldSize), newSize, empty);
        }
        destFile.getFD().sync();
        destFile.close();
        sourceFile.close();
    }

    private void init() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/init ...");
        nFree.set(0);
        filename = path + File.separatorChar + FILENAME;
        file = new RandomAccessFile(filename, "rw");
        fileLength = file.length();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/init, fileLength=" + fileLength);
        if (fileLength > 0) {
            numberPages.set((int) (fileLength / PageSize.getCurrent()));
            if (!offline)
                shrinkFile();
        } else
            initialize(initialPages);
        sync();
        if (freePoolEnabled)
            buildFreePageList();
        if (!offline) {
            Entity entity = ctx.filesList.createEntity();
            entity.setName(FILENAME);
            entity.createCommands();
            ctx.filesList.addEntity(entity);
            freeProp = entity.getProperty("free-pages");
            usedProp = entity.getProperty("used-pages");
            fileSizeProp = entity.getProperty("file-size");
            sizeCollectProp = ctx.dbEntity.getProperty("size-collect-interval");
            collectInterval = (long) (Long) sizeCollectProp.getValue();
            startTimer();
            PropertyChangeListener propChangeListener = (property, oldValue, newValue) -> {
                stopTimer();
                collectInterval = (Long) newValue;
                startTimer();
            };
            sizeCollectProp.setPropertyChangeListener(propChangeListener);
            swiftletManagerAdapter = new SwiftletManagerAdapter() {
                public void swiftletStarted(SwiftletManagerEvent event) {
                    mgmtSwiftlet = (MgmtSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$mgmt");
                    mgmtSwiftlet.addMgmtListener(StableStore.this);
                }
            };
            SwiftletManager.getInstance().addSwiftletManagerListener("sys$mgmt", swiftletManagerAdapter);
        }
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/init done, pageSize=" + PageSize.getCurrent() + ", size=" + numberPages + ", freePages=" + getNumberFreePages());
    }

    private void panic(Exception e) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/panic: " + e);
        System.err.println("PANIC, EXITING: " + e);
        e.printStackTrace();
        SwiftletManager.getInstance().disableShutdownHook();
        System.exit(-1);
    }

    private void addToFreePool(int pageNo) {
        if (!freePoolEnabled)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/addToFreePool, pageNo=" + pageNo);
        freePages.offer(pageNo);
        nFree.getAndIncrement();
    }

    private int getFirstFree() {
        if (!freePoolEnabled || nFree.get() == 0)
            return -1;
        Integer pageNo = freePages.poll();
        nFree.getAndDecrement();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/getFirstFree, pageNo=" + pageNo);
        return pageNo;
    }

    private void initialize(int nPages) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/initialize, nPages=" + nPages);
        if (numberPages.get() > 0)
            throwException("can't initialize - page store contains " + numberPages + " pages");
        ensurePage(nPages - 1);
        for (int i = 0; i < nPages; i++) {
            Page p = createPage(i);
            writePage(p);
        }
        numberPages.set(nPages);
    }

    public void setNumberPages(int numberPages) {
        this.numberPages.set(numberPages);
    }

    public Page createPage(int pageNo) {
        Page p = new Page();
        p.pageNo = pageNo;
        p.empty = true;
        p.dirty = false;
        p.data = new byte[PageSize.getCurrent()];
        return p;
    }

    protected void ensurePage(int pageNo) throws Exception {
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

    public void sync() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/sync...");
        file.getFD().sync();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/sync done.");
    }

    public void truncateToPage(int pageNo) throws Exception {
        try {
            long length = (long) (pageNo + 1) * (long) PageSize.getCurrent();
            file.setLength(length);
            fileLength = length;
        } catch (Exception e) {
            panic(e);
        }
    }

    private void buildFreePageList() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/buildFreePageList...");
        for (int i = 0; i < numberPages.get(); i++) {
            Page p = loadPage(i);
            if (p.empty && i > 0) // never put the root index page into the free page list!
            {
                addToFreePool(p.pageNo);
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/buildFreePageList, freePageNumbers.size=" + getNumberFreePages());
    }

    private void throwException(String msg) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/throwException: " + msg);
        throw new Exception(msg);
    }

    protected Page loadPage(int pageNo) throws Exception {
        Page p = createPage(pageNo);
        long seekPoint = (long) pageNo * (long) PageSize.getCurrent();
        position(seekPoint, p.data.length);
        file.read(p.data);
        p.empty = p.data[0] == 1;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/loadPage, pageNo=" + pageNo + ", empty=" + p.empty);
        return p;
    }

    public int getNumberPages() {
        return numberPages.get();
    }

    int getNumberFreePages() {
        return nFree.get();
    }

    protected Page create() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/create ...");
        Page p = null;
        int pn = getFirstFree();
        if (pn != -1) {
            p = createPage(pn);
        } else {
            p = createPage(numberPages.get());
            numberPages.getAndIncrement();
            writePage(p);
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/create, page=" + p);
        return p;
    }

    public void ensure(int pageNo) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/ensure, pageNo=" + pageNo);
        if (pageNo > numberPages.get() - 1) {
            ensurePage(pageNo);
            for (int i = numberPages.get(); i <= pageNo; i++) {
                Page p = createPage(i);
                writePage(p);
                addToFreePool(pageNo);
            }
            numberPages.set(pageNo + 1);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/ensure, numberPages=" + numberPages);
    }

    public Page get(int pageNo) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/get, pageNo=" + pageNo);
        if (pageNo > numberPages.get())
            throwException("get pageNo " + pageNo + " is out of range [0.." + (numberPages) + "]");
        return loadPage(pageNo);
    }

    public void put(Page page) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/put, page=" + page);
        if (page.pageNo > numberPages.get())
            throwException("put pageNo " + page.pageNo + " is out of range [0.." + (numberPages) + "]");
        writePage(page);
    }

    public void free(Page page) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/free, page=" + page);
        if (page.pageNo > numberPages.get())
            throwException("free pageNo " + page.pageNo + " is out of range [0.." + (numberPages) + "]");
        page.empty = true;
        writePage(page);
        if (freePoolEnabled)
            addToFreePool(page.pageNo);
    }

    protected void writePage(Page p) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/writePage, page=" + p);
        try {
            if (p.empty)
                p.data = new byte[PageSize.getCurrent()];
            p.data[0] = (byte) (p.empty ? 1 : 0);
            long seekPoint = (long) p.pageNo * (long) PageSize.getCurrent();
            position(seekPoint, p.data.length);
            file.write(p.data);
            p.dirty = false;
            long length = (long) (p.pageNo + 1) * (long) PageSize.getCurrent();
            if (length > fileLength)
                fileLength = length;
        } catch (Exception e) {
            panic(e);
        }
    }

    public void shrinkFile() throws Exception {
        int shrinked = numberPages.get();
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/shrinkFile, before: numberPages=" + numberPages);
        for (int i = numberPages.get() - 1; i >= initialPages; i--) {
            Page p = loadPage(i);
            if (p.empty)
                shrinked--;
            else
                break;
        }
        numberPages.set(shrinked);
        truncateToPage(numberPages.get() - 1);
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/shrinkFile, after: numberPages=" + numberPages);
    }

    public void shrink() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/shrink...");
        shrinkFile();
        freePages.clear();
        nFree.set(0);
        buildFreePageList();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/shrink done");
    }

    public void deleteStore() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/deleteStore");
        freePages.clear();
        numberPages.set(0);
        file.setLength(0);
        fileLength = 0;
    }

    public void copy(String destPath) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/copy to " + (destPath + File.separatorChar + FILENAME) + " ...");
        int nPages = (int) (file.length() / PageSize.getCurrent());
        int shrinked = nPages;
        for (int i = nPages - 1; i >= 0; i--) {
            Page p = loadPage(i);
            if (p.empty)
                shrinked--;
            else
                break;
        }
        byte[] empty = makeEmptyArray(PageSize.getCurrent());
        String destFilename = destPath + File.separatorChar + FILENAME;
        RandomAccessFile destFile = new RandomAccessFile(destFilename, "rw");
        for (int i = 0; i < shrinked; i++) {
            writePage(destFile, loadPage(i), PageSize.getCurrent(), empty);
        }
        destFile.getFD().sync();
        destFile.close();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/copy to " + (destPath + File.separatorChar + FILENAME) + " done");
    }

    public void close() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/close");
        if (!offline) {
            sizeCollectProp.setPropertyChangeListener(null);
            SwiftletManager.getInstance().removeSwiftletManagerListener("sys$mgmt", swiftletManagerAdapter);
            if (mgmtSwiftlet != null)
                mgmtSwiftlet.removeMgmtListener(this);
            stopTimer();
        }
        if (freePoolEnabled)
            freePages.clear();
        numberPages.set(0);
        fileLength = 0;
        file.close();
    }

    public void reset() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/reset");
        close();
        init();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/reset done");
    }

    public String toString() {
        return "StableStore, file=" + filename + ", offline=" + offline + ", freePoolEnabled=" + freePoolEnabled;
    }
}

