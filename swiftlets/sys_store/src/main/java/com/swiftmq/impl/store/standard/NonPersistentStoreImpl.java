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

package com.swiftmq.impl.store.standard;

import com.swiftmq.impl.store.standard.swap.SwapAddress;
import com.swiftmq.impl.store.standard.swap.SwapFile;
import com.swiftmq.swiftlet.store.NonPersistentStore;
import com.swiftmq.swiftlet.store.StoreEntry;
import com.swiftmq.swiftlet.store.StoreException;

import java.io.File;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class NonPersistentStoreImpl implements NonPersistentStore {
    static final MessageFormat format = new MessageFormat("{0}-{1,number,integer}.swap");
    StoreContext ctx = null;
    protected String queueName = null;
    String path = null;
    long maxLength = 0;
    List<SwapFile> swapFiles = null;
    SwapFile actSwapFile = null;
    int swapFileCount = 0;

    protected NonPersistentStoreImpl(StoreContext ctx, String queueName, String path, long maxLength) {
        this.ctx = ctx;
        this.queueName = queueName;
        this.path = path;
        new File(this.path).mkdirs();
        this.maxLength = maxLength;
        if (swapFiles == null)
            swapFiles = new ArrayList<>();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/created");
    }

    private void checkSwapFile() throws Exception {
        if (actSwapFile == null || !actSwapFile.hasSpace()) {
            actSwapFile = ctx.swapFileFactory.createSwapFile(path, format.format(new Object[]{queueName, (long) swapFileCount++}), maxLength);
            swapFiles.add(actSwapFile);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", toString() + "/checkSwapFile, new swapFile=" + actSwapFile);
        }
    }

    protected boolean deleteSwapFilesOnClose() {
        return true;
    }

    public StoreEntry get(Object key)
            throws StoreException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/get, key=" + key);
        StoreEntry entry = null;
        try {
            SwapAddress sa = (SwapAddress) key;
            entry = sa.swapFile.get(sa.filePointer);
            entry.key = key;
        } catch (Exception e) {
            throw new StoreException(e.getMessage());
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/get done, key=" + key + ", entry=" + entry);
        return entry;
    }

    public void updateDeliveryCount(Object key, int deliveryCount)
            throws StoreException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/updateDeliveryCount, key=" + key + ", deliveryCount=" + deliveryCount);
        try {
            SwapAddress sa = (SwapAddress) key;
            sa.swapFile.updateDeliveryCount(sa.filePointer, deliveryCount);
        } catch (Exception e) {
            throw new StoreException(e.getMessage());
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/updateDeliveryCount done, key=" + key + ", deliveryCount=" + deliveryCount);
    }

    public void insert(StoreEntry storeEntry)
            throws StoreException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/insert, storeEntry=" + storeEntry);
        try {
            checkSwapFile();
            SwapAddress sa = new SwapAddress();
            sa.swapFile = actSwapFile;
            sa.filePointer = sa.swapFile.add(storeEntry);
            storeEntry.key = sa;
        } catch (Exception e) {
            throw new StoreException(e.getMessage());
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/insert done, storeEntry=" + storeEntry);
    }

    public void delete(Object key)
            throws StoreException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/delete, key=" + key);
        try {
            SwapAddress sa = (SwapAddress) key;
            sa.swapFile.remove(sa.filePointer);
            if (sa.swapFile.getNumberMessages() == 0) {
                sa.swapFile.close();
                swapFiles.remove(sa.swapFile);
                if (actSwapFile == sa.swapFile)
                    actSwapFile = null;
            }
        } catch (Exception e) {
            throw new StoreException(e.getMessage());
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/delete done, key=" + key);
    }

    public void close()
            throws StoreException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/close...");
        try {
            if (swapFiles != null) {
                if (deleteSwapFilesOnClose()) {
                    for (SwapFile swapFile : swapFiles) {
                        swapFile.close();
                    }
                }
                swapFiles.clear();
                swapFiles = null;
                actSwapFile = null;
            }
        } catch (Exception e) {
            throw new StoreException(e.getMessage());
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/close...done");
    }

    public String toString() {
        return "[NonPersistentStoreImpl, queueName=" + queueName + ", swapFiles.size()=" + (swapFiles == null ? 0 : swapFiles.size()) + ", swapFileCount=" + swapFileCount + "]";
    }
}

