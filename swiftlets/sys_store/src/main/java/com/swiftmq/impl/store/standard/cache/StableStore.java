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
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.Property;
import com.swiftmq.mgmt.PropertyChangeException;
import com.swiftmq.mgmt.PropertyChangeListener;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.event.SwiftletManagerAdapter;
import com.swiftmq.swiftlet.event.SwiftletManagerEvent;
import com.swiftmq.swiftlet.mgmt.MgmtSwiftlet;
import com.swiftmq.swiftlet.mgmt.event.MgmtListener;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.collection.IntRingBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class StableStore implements TimerListener, MgmtListener {
    public static final String FILENAME = "page.db";
    protected StoreContext ctx;
    protected String path;
    protected IntRingBuffer freePool;
    protected RandomAccessFile file;
    protected String filename;
    protected volatile long fileLength;
    protected volatile int numberPages = 0;
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
    private boolean syncPageDbEnabled = true;

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

    private void init() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/init ...");
        if (freePoolEnabled)
            freePool = new IntRingBuffer(16384);
        filename = path + File.separatorChar + FILENAME;
        emptyData = new byte[Page.PAGE_SIZE];
        emptyData[0] = 1; // emtpy
        for (int i = 1; i < Page.PAGE_SIZE; i++)
            emptyData[i] = 0;
        file = new RandomAccessFile(filename, "rw");
        fileLength = file.length();
        if (fileLength > 0) {
            numberPages = (int) (fileLength / Page.PAGE_SIZE);
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
            collectInterval = (long) ((Long) sizeCollectProp.getValue()).longValue();
            startTimer();
            PropertyChangeListener propChangeListener = new PropertyChangeListener() {
                public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                    stopTimer();
                    collectInterval = ((Long) newValue).longValue();
                    startTimer();
                }
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
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/init done, size=" + numberPages + ", freePages=" + getNumberFreePages());
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

    public void setSyncPageDbEnabled(boolean syncPageDbEnabled) {
        this.syncPageDbEnabled = syncPageDbEnabled;
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
            freeProp.setValue(freePool.getSize());
            usedProp.setValue(numberPages - freePool.getSize());
            fileSizeProp.setValue((int) (fileLength / (1024 * 1024)));
        } catch (Exception e) {
        }
    }

    private void addToFreePool(int pageNo) {
        if (!freePoolEnabled)
            return;
        freePool.add(pageNo);
    }

    private int getFirstFree() {
        if (!freePoolEnabled)
            return -1;
        return freePool.getSize() > 0 ? freePool.remove() : -1;
    }

    private void seek(int pageNo) throws Exception {
        long seekPoint = (long) pageNo * (long) Page.PAGE_SIZE;
        file.seek(seekPoint);
    }

    private void initialize(int nPages) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/initialize, nPages=" + nPages);
        if (numberPages > 0)
            throwException("can't initialize - page store contains " + numberPages + " pages");
        ensurePage(nPages - 1);
        for (int i = 0; i < nPages; i++) {
            Page p = createPage(i);
            writePage(p);
        }
        numberPages = nPages;
    }

    private void panic(Exception e) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/panic: " + e);
        System.err.println("PANIC, EXITING: " + e);
        e.printStackTrace();
        SwiftletManager.getInstance().disableShutdownHook();
        System.exit(-1);
    }

    public Page createPage(int pageNo) {
        Page p = new Page();
        p.pageNo = pageNo;
        p.empty = true;
        p.dirty = false;
        p.data = new byte[Page.PAGE_SIZE];
        System.arraycopy(emptyData, 0, p.data, 0, emptyData.length);
        return p;
    }

    protected void ensurePage(int pageNo) throws Exception {
        long length = (long) (pageNo + 1) * (long) Page.PAGE_SIZE;
        if (fileLength < length) {
            try {
                file.setLength(length);
                fileLength = length;
            } catch (Exception e) {
                panic(e);
            }
        }
    }

    public void truncateToPage(int pageNo) throws Exception {
        try {
            long length = (long) (pageNo + 1) * (long) Page.PAGE_SIZE;
            file.setLength(length);
            fileLength = length;
        } catch (Exception e) {
            panic(e);
        }
    }

    public void setNumberPages(int numberPages) {
        this.numberPages = numberPages;
    }

    protected Page loadPage(int pageNo) throws Exception {
        Page p = createPage(pageNo);
        p.data = new byte[Page.PAGE_SIZE];
        seek(pageNo);
        file.readFully(p.data);
        p.empty = p.data[0] == 1;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/loadPage, pageNo=" + pageNo + ", empty=" + p.empty);
        return p;
    }

    protected void writePage(Page p) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/writePage, page=" + p);
        try {
            if (p.empty)
                System.arraycopy(emptyData, 0, p.data, 0, emptyData.length);
            p.data[0] = (byte) (p.empty ? 1 : 0);
            seek(p.pageNo);
            file.write(p.data);
            p.dirty = false;
            long length = (long) (p.pageNo + 1) * (long) Page.PAGE_SIZE;
            if (length > fileLength)
                fileLength = length;
        } catch (Exception e) {
            panic(e);
        }
    }

    protected void writePage(RandomAccessFile f, Page p) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/writePage, page=" + p);
        try {
            if (p.empty)
                System.arraycopy(emptyData, 0, p.data, 0, emptyData.length);
            p.data[0] = (byte) (p.empty ? 1 : 0);
            seek(p.pageNo);
            f.write(p.data);
            p.dirty = false;
        } catch (IOException e) {
            panic(e);
        }
    }

    public void sync() throws Exception {
        if (!syncPageDbEnabled)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/sync...");
        file.getFD().sync();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/sync done.");
    }

    public void shrinkFile() throws Exception {
        int shrinked = numberPages;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/shrinkFile, before: numberPages=" + numberPages);
        for (int i = numberPages - 1; i >= initialPages; i--) {
            Page p = loadPage(i);
            if (p.empty)
                shrinked--;
            else
                break;
        }
        numberPages = shrinked;
        truncateToPage(numberPages - 1);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/shrinkFile, after: numberPages=" + numberPages);
    }

    private void buildFreePageList() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/buildFreePageList...");
        for (int i = 0; i < numberPages; i++) {
            Page p = loadPage(i);
            if (p.empty && i > 0) // never put the root index page into the free page list!{
                addToFreePool(p.pageNo);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/buildFreePageList, freePageNumbers.size=" + getNumberFreePages());
    }

    private void throwException(String msg) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/throwException: " + msg);
        throw new Exception(msg);
    }

    public void shrink() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/shrink...");
        shrinkFile();
        freePool = new IntRingBuffer(16384);
        buildFreePageList();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/shrink done");
    }

    public int getNumberPages() {
        return numberPages;
    }

    int getNumberFreePages() {
        return freePoolEnabled ? freePool.getSize() : 0;
    }

    protected Page create() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/create ...");
        Page p = null;
        int pn = getFirstFree();
        if (pn != -1) {
            p = createPage(pn);
        } else {
            p = createPage(numberPages);
            numberPages++;
            writePage(p);
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/create, page=" + p);
        return p;
    }

    public void ensure(int pageNo) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/ensure, pageNo=" + pageNo);
        if (pageNo > numberPages - 1) {
            ensurePage(pageNo);
            for (int i = numberPages; i <= pageNo; i++) {
                Page p = createPage(i);
                writePage(p);
                addToFreePool(pageNo);
            }
            numberPages = pageNo + 1;
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/ensure, numberPages=" + numberPages);
    }

    public Page get(int pageNo) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/get, pageNo=" + pageNo);
        if (pageNo > numberPages)
            throwException("get pageNo " + pageNo + " is out of range [0.." + (numberPages) + "]");
        return loadPage(pageNo);
    }

    public void put(Page page) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/put, page=" + page);
        if (page.pageNo > numberPages)
            throwException("put pageNo " + page.pageNo + " is out of range [0.." + (numberPages) + "]");
        writePage(page);
    }

    public void free(Page page) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/free, page=" + page);
        if (page.pageNo > numberPages)
            throwException("free pageNo " + page.pageNo + " is out of range [0.." + (numberPages) + "]");
        page.empty = true;
        writePage(page);
        if (freePoolEnabled)
            addToFreePool(page.pageNo);
    }

    public void free(int pageNo) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/free, pageNo=" + pageNo);
        if (pageNo > numberPages)
            throwException("free pageNo " + pageNo + " is out of range [0.." + (numberPages) + "]");
        writePage(createPage(pageNo));
        if (freePoolEnabled)
            addToFreePool(pageNo);
    }

    public void deleteStore() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/deleteStore");
        freePool.clear();
        numberPages = 0;
        file.setLength(0);
        fileLength = 0;
    }

    public void copy(String destPath) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/copy to " + (destPath + File.separatorChar + FILENAME) + " ...");
        int nPages = (int) (file.length() / Page.PAGE_SIZE);
        int shrinked = nPages;
        for (int i = nPages - 1; i >= 0; i--) {
            Page p = loadPage(i);
            if (p.empty)
                shrinked--;
            else
                break;
        }
        String destFilename = destPath + File.separatorChar + FILENAME;
        RandomAccessFile destFile = new RandomAccessFile(destFilename, "rw");
        for (int i = 0; i < shrinked; i++) {
            writePage(destFile, loadPage(i));
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
            freePool.clear();
        numberPages = 0;
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

