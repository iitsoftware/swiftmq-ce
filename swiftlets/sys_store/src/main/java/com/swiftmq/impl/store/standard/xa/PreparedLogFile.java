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

package com.swiftmq.impl.store.standard.xa;

import com.swiftmq.impl.store.standard.StoreContext;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PreparedLogFile extends PreparedLog {
    static final String FILENAME = "xa.log";
    StoreContext ctx = null;
    String path = null;
    String filename = null;
    final AtomicBoolean autoSync = new AtomicBoolean(false);
    RandomAccessFile file = null;
    final AtomicInteger validRecords = new AtomicInteger();

    public PreparedLogFile(StoreContext ctx, String path, boolean autoSync) throws IOException {
        this.ctx = ctx;
        this.path = path;
        this.autoSync.set(autoSync);
        filename = path + File.separatorChar + FILENAME;
        new File(filename).getParentFile().mkdirs();
        file = new RandomAccessFile(path + File.separatorChar + FILENAME, "rw");
        getAll();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/created, " + validRecords + " prepared transactions");
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/created, " + validRecords + " prepared transactions");
    }

    public long add(PrepareLogRecordImpl logRecord) throws IOException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/add, logRecord: " + logRecord);
        file.seek(file.length());
        long address = file.getFilePointer();
        logRecord.setAddress(address);
        logRecord.writeContent(file);
        validRecords.getAndIncrement();
        if (autoSync.get())
            file.getFD().sync();
        return address;
    }

    public PrepareLogRecordImpl get(long address) throws IOException {
        file.seek(address);
        PrepareLogRecordImpl logRecord = new PrepareLogRecordImpl(address);
        logRecord.readContent(file);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/get, logRecord: " + logRecord);
        return logRecord;
    }

    public List<PrepareLogRecordImpl> getAll() throws IOException {
        List<PrepareLogRecordImpl> list = new ArrayList();
        file.seek(0);
        while (file.getFilePointer() < file.length()) {
            try {
                PrepareLogRecordImpl logRecord = new PrepareLogRecordImpl(file.getFilePointer());
                logRecord.readContent(file);
                if (logRecord.isValid())
                    list.add(logRecord);
            } catch (EOFException e) {
                System.err.println("+++ WARNING! Got an EOFException while reading " + filename);
                System.err.println("+++          Unable to reconstruct the last prepared Log Record!");
            }
        }
        validRecords.set(list.size());
        if (validRecords.get() == 0) {
            file.setLength(0);
            if (autoSync.get())
                file.getFD().sync();
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/getAll, logRecords: " + list);
        return list;
    }

    public void remove(PrepareLogRecordImpl logRecord) throws IOException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/remove, logRecord: " + logRecord);
        file.seek(logRecord.getAddress());
        logRecord.setValid(false);
        logRecord.writeValid(file);
        validRecords.getAndDecrement();
        if (validRecords.get() == 0)
            file.setLength(0);
        if (autoSync.get())
            file.getFD().sync();
    }

    public boolean backupRequired() {
        return true;
    }

    public void backup(String destPath) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/copy to " + (destPath + File.separatorChar + FILENAME) + " ...");
        String destFilename = destPath + File.separatorChar + FILENAME;
        RandomAccessFile destFile = new RandomAccessFile(destFilename, "rw");
        destFile.seek(0);
        int SIZE = 8192;
        int n = 0;
        int pos = 0;
        byte[] b = new byte[SIZE];
        file.seek(0);
        while ((n = file.read(b)) != -1) {
            destFile.write(b, 0, n);
            pos += n;
            destFile.seek(pos);
            file.seek(pos);
        }
        destFile.getFD().sync();
        destFile.close();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/copy to " + (destPath + File.separatorChar + FILENAME) + " done");
    }

    public void sync() throws IOException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/sync");
        file.getFD().sync();
    }

    public String toString() {
        return "PreparedLogFile, filename=" + filename;
    }
}
