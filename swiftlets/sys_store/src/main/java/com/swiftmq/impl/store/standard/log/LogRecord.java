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

import com.swiftmq.impl.store.standard.cache.CacheReleaseListener;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;
import com.swiftmq.tools.concurrent.Semaphore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class LogRecord extends LogOperation {
    public final static int START = 1;
    public final static int COMMIT = 2;
    public final static int ABORT = 3;
    long magic = 0;
    long txId;
    Semaphore semaphore;
    List journal = null;
    boolean complete = false;
    CacheReleaseListener cacheReleaseListener = null;
    AsyncCompletionCallback callback = null;
    List messagePageRefs = null;

    public LogRecord(long txId, Semaphore semaphore, List journal, CacheReleaseListener cacheReleaseListener, AsyncCompletionCallback callback, List messagePageRefs) {
        this.txId = txId;
        this.semaphore = semaphore;
        this.journal = journal;
        this.cacheReleaseListener = cacheReleaseListener;
        this.callback = callback;
        this.messagePageRefs = messagePageRefs;
    }

    public static LogRecord create(int type) {
        LogRecord lr = null;
        switch (type) {
            case START:
                lr = new StartLogRecord(0);
                break;
            case COMMIT:
                lr = new CommitLogRecord(0, null, null, null, null);
                break;
            case ABORT:
                lr = new AbortLogRecord(0, null, null, null, null);
                break;
        }
        return lr;
    }

    public long getTxId() {
        return (txId);
    }

    public Semaphore getSemaphore() {
        return (semaphore);
    }

    int getOperationType() {
        return OPER_LOG_REC;
    }

    public abstract int getLogType();

    public List getJournal() {
        return (journal);
    }

    public CacheReleaseListener getCacheReleaseListener() {
        return cacheReleaseListener;
    }

    public AsyncCompletionCallback getCallback() {
        return callback;
    }

    public List getMessagePageRefs() {
        return messagePageRefs;
    }

    public boolean isComplete() {
        return complete;
    }

    public long getMagic() {
        return magic;
    }

    public void setMagic(long magic) {
        this.magic = magic;
    }

    public void writeContent(DataOutput out) throws IOException {
        writeContent(out, false);
    }

    public void writeContent(DataOutput out, boolean includeMagic) throws IOException {
        if (includeMagic)
            out.writeLong(magic);
        out.writeLong(txId);
        out.writeInt(journal.size());
        for (int i = 0; i < journal.size(); i++) {
            LogAction action = (LogAction) journal.get(i);
            out.writeInt(action.getType());
            action.writeContent(out);
        }
    }

    public void readContent(DataInput in) throws IOException {
        readContent(in, false);
    }

    public void readContent(DataInput in, boolean includeMagic) throws IOException {
        if (includeMagic)
            magic = in.readLong();
        txId = in.readLong();
        int n = in.readInt();
        journal = new ArrayList(n);
        for (int i = 0; i < n; i++) {
            LogAction la = LogAction.create(in.readInt());
            la.readContent(in);
            journal.add(la);
        }
        complete = true;
    }

    public String toString() {
        return "journal=" + journal;
    }
}

