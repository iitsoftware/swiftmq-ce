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
import com.swiftmq.tools.util.DataByteBufferOutputStream;

import java.io.DataOutput;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ReuseLogFile implements LogFile {
    static final String PROP_VERBOSE = "swiftmq.store.checkpoint.verbose";
    static final int BUFFER_SIZE = 1024 * 1024 * 10;
    private static boolean DIRECT_BUFFERS = Boolean.valueOf(System.getProperty("swiftmq.store.usedirectbuffers", "true")).booleanValue();
    boolean checkPointVerbose = Boolean.getBoolean(PROP_VERBOSE);
    StoreContext ctx;
    RandomAccessFile file = null;
    DataByteBufferOutputStream outStream = null;
    FileChannel fileChannel = null;
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
        fileChannel = file.getChannel();
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
            boolean direct = DIRECT_BUFFERS;
            outStream = new DataByteBufferOutputStream(BUFFER_SIZE, BUFFER_SIZE, direct);
            outStream.setTrace(ctx.traceSpace, "sys$store");
            if (file.length() < maxSize) {
                file.setLength(maxSize);
                file.seek(0);
                byte[] b = new byte[BUFFER_SIZE];
                for (int i = 0; i < b.length; i++)
                    b[i] = 0;
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
            ByteBuffer byteBuffer = outStream.getBuffer();
            byteBuffer.flip();
            fileChannel.write(byteBuffer);
            outStream.rewind();
            if (sync) {
                long s1 = System.currentTimeMillis();
                fileChannel.force(false);
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
