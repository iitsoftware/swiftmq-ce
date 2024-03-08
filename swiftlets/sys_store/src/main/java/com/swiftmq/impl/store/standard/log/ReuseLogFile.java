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

package com.swiftmq.impl.store.standard.log;

import com.swiftmq.impl.store.standard.StoreContext;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.DataOutput;
import java.io.IOException;
import java.io.RandomAccessFile;

public class ReuseLogFile implements LogFile {
    static final String PROP_VERBOSE = "swiftmq.store.checkpoint.verbose";
    static final int BUFFER_SIZE = 1024 * 1024 * 10;
    boolean checkPointVerbose = Boolean.getBoolean(PROP_VERBOSE);
    StoreContext ctx;
    RandomAccessFile file = null;
    DataByteArrayOutputStream outStream = null;
    long position = 0;
    long magic = System.currentTimeMillis();
    int nFlushes = 0;
    int nLogRecs = 0;
    long totalWriteSize = 0;
    long msSync = 0;
    boolean inMemoryMode = false;

    public ReuseLogFile(StoreContext ctx, RandomAccessFile file) {
        this.ctx = ctx;
        this.file = file;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/created");
    }

    public void setInMemoryMode(boolean inMemoryMode) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/setInMemoryMode=" + inMemoryMode);
        // to avoid synchronization, flush is done from caller if inMemoryMode is set to false
        this.inMemoryMode = inMemoryMode;
    }

    public boolean isInMemoryMode() {
        return inMemoryMode;
    }

    public RandomAccessFile getFile() {
        return file;
    }

    public int getFlushSize() {
        return outStream.getCount();
    }

    public long getPosition() {
        return inMemoryMode ? position + outStream.getCount() : position;
    }

    public void init(long maxSize) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/init...");
        try {
            outStream = new DataByteArrayOutputStream(BUFFER_SIZE, BUFFER_SIZE);
            if (file.length() < maxSize) {
                file.setLength(maxSize);
                file.seek(0);
                byte[] b = new byte[BUFFER_SIZE];
                position = 0;
                while (position < maxSize) {
                    int len = Math.min((int) (maxSize - position), b.length);
                    outStream.write(b, 0, len);
                    flush(false);
                    position += len;
                }
            }
            reset(true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/init done");
    }

    public void write(LogRecord logRecord) throws IOException {
        write(logRecord, null);
    }

    public void write(LogRecord logRecord, DataOutput copyHere) throws IOException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/write, logRecord=" + logRecord + " ...");
        logRecord.setMagic(magic);
        outStream.writeInt(logRecord.getLogType());
        logRecord.writeContent(outStream, true);
        if (copyHere != null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", toString() + "/write logRecord to copyHere");
            copyHere.writeInt(logRecord.getLogType());
            logRecord.writeContent(copyHere, true);
        }
        nLogRecs++;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/write, logRecord=" + logRecord + " done");
    }

    public void flush(boolean sync) throws IOException {
        if (!inMemoryMode) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/flush, sync=" + sync + " ...");
            nFlushes++;
            totalWriteSize += outStream.getCount();
            position += outStream.getCount();
            file.write(outStream.getBuffer(), 0, outStream.getCount());
            outStream.rewind();
            if (sync) {
                long s1 = System.currentTimeMillis();
                file.getFD().sync();
                msSync += (System.currentTimeMillis() - s1);
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", toString() + "/flush, sync=" + sync + " done");
        } else if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/flush, sync=" + sync + " do nothing (inMemoryode)");

    }

    public void reset(boolean sync) throws IOException {
        reset(sync, null);
    }

    public void reset(boolean sync, DataOutput copyHere) throws IOException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/reset, sync=" + sync + " ...");
        if (inMemoryMode)
            outStream.rewind();
        file.seek(0);
        position = 0;
        if (magic == Long.MAX_VALUE)
            magic = 0;
        magic++;
        write(new StartLogRecord(magic), copyHere);
        flush(sync);
        if (checkPointVerbose && nFlushes > 0)
            System.out.println("ReuseLogFile, nFlushes: " + nFlushes + ", avg # log recs: " + (nLogRecs / nFlushes) + ", avg write size: " + (totalWriteSize / nFlushes) + (msSync > 0 ? ", avg ms sync: " + (msSync / nFlushes) : ""));
        nFlushes = 0;
        nLogRecs = 0;
        totalWriteSize = 0;
        msSync = 0;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/reset, sync=" + sync + " done");
    }

    public String toString() {
        return "ReuseLogFile, inMemoryMode=" + inMemoryMode + ", position=" + position + ", buffered=" + (outStream == null ? 0 : outStream.getCount()) + ", magic=" + magic;
    }
}
