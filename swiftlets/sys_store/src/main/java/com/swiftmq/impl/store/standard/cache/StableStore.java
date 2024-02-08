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
import com.swiftmq.impl.store.standard.cache.pagedb.FileAccess;
import com.swiftmq.impl.store.standard.cache.pagedb.MappedFileRegionController;
import com.swiftmq.impl.store.standard.cache.pagedb.PageDB;
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

public class StableStore implements TimerListener, MgmtListener {
    protected StoreContext ctx;
    private FileAccess fileAccess;
    protected String path;
    private Property sizeCollectProp = null;
    private SwiftletManagerAdapter swiftletManagerAdapter = null;
    private Property freeProp = null;
    private Property usedProp = null;
    private Property fileSizeProp = null;
    private volatile long collectInterval = 0;
    private volatile boolean mgmtToolAttached = false;
    private volatile boolean timerRunning = false;
    private MgmtSwiftlet mgmtSwiftlet = null;
    private boolean offline;

    public StableStore(StoreContext ctx, String path, int initialPages) throws Exception {
        this(ctx, path, initialPages, false, true);
    }

    public StableStore(StoreContext ctx, String path, int initialPages, boolean offline) throws Exception {
        this(ctx, path, initialPages, offline, true);
    }

    public StableStore(StoreContext ctx, String path, int initialPages, boolean offline, boolean freePoolEnabled) throws Exception {
        this.ctx = ctx;
        this.offline = offline;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/creating ...");
        fileAccess = new MappedFileRegionController(ctx, path, 10000, freePoolEnabled);
        init();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/creating done.");
    }

    private void startTimer() {
        if (mgmtToolAttached && collectInterval > 0) {
            ctx.timerSwiftlet.addTimerListener(collectInterval, this);
            timerRunning = true;
        }
    }

    private void stopTimer() {
        if (timerRunning) {
            ctx.timerSwiftlet.removeTimerListener(this);
            timerRunning = false;
        }
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
            freeProp.setValue(fileAccess.freePages());
            usedProp.setValue(fileAccess.usedPages());
            fileSizeProp.setValue(fileAccess.fileSize() / 1024);
        } catch (Exception ignored) {
        }
    }

    private void init() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/init ...");
        fileAccess.init();
        if (!offline) {
            Entity entity = ctx.filesList.createEntity();
            entity.setName(PageDB.FILENAME);
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
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/init done, pageSize=" + PageSize.getCurrent() + ", size=" + fileAccess.numberPages() + ", freePages=" + getNumberFreePages());
    }

    private void throwException(String msg) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/throwException: " + msg);
        throw new Exception(msg);
    }

    public Page createPage(int pageNo) {
        return fileAccess.initPage(pageNo);
    }

    protected void ensurePage(int pageNo) throws Exception {
        fileAccess.ensurePage(pageNo);
    }

    public void sync() throws Exception {
        fileAccess.sync();
    }

    public int getNumberPages() {
        return fileAccess.numberPages();
    }

    int getNumberFreePages() {
        return fileAccess.freePages();
    }

    protected Page create() throws Exception {
        return fileAccess.createPage();
    }

    public void ensure(int pageNo) throws Exception {
        fileAccess.ensurePage(pageNo);
    }

    public Page get(int pageNo) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/get, pageNo=" + pageNo);
        if (pageNo > fileAccess.numberPages())
            throwException("get pageNo " + pageNo + " is out of range [0.." + (fileAccess.numberPages()) + "]");
        return fileAccess.loadPage(pageNo);
    }

    public void put(Page page) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/put, page=" + page);
        if (page.pageNo > fileAccess.numberPages())
            throwException("put pageNo " + page.pageNo + " is out of range [0.." + (fileAccess.numberPages()) + "]");
        fileAccess.writePage(page);
    }

    public void free(Page page) throws Exception {
        fileAccess.freePage(page);
    }

    public void shrink() throws Exception {
        fileAccess.shrink();
    }

    public void deleteStore() throws Exception {
        fileAccess.deleteStore();
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
        fileAccess.close();
    }

    public void reset() throws Exception {
        fileAccess.reset();
    }

    public String toString() {
        return "StableStore, fileAccess=" + fileAccess;
    }
}

