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

package com.swiftmq.impl.store.standard.recover;

import com.swiftmq.impl.store.standard.StoreContext;
import com.swiftmq.impl.store.standard.cache.Page;
import com.swiftmq.impl.store.standard.index.IndexAnalyzer;
import com.swiftmq.impl.store.standard.log.*;
import com.swiftmq.impl.store.standard.pagesize.PageSize;
import com.swiftmq.tools.concurrent.Semaphore;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.List;

public class RecoveryManager {
    static final String PROP_ANALYZE = "swiftmq.store.analyze";
    StoreContext ctx;
    Semaphore checkPointBarriere = null;
    int commits = 0;
    int rollbacks = 0;
    int prepares = 0;
    long magic = -1;
    byte[] emptyData = null;

    public RecoveryManager(StoreContext ctx) {
        this.ctx = ctx;
        emptyData = new byte[PageSize.getCurrent()];
        emptyData[0] = 1; // emtpy
        for (int i = 1; i < PageSize.getCurrent(); i++)
            emptyData[i] = 0;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/create");
    }

    private void processCommit(List journal) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/processCommit...");

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/processCommit, " + journal.size() + " actions rolling forward");
        commits++;
        for (int i = 0; i < journal.size(); i++) {
            LogAction la = (LogAction) journal.get(i);
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/processCommit, " + la);
            switch (la.getType()) {
                case LogAction.INSERT: {
                    Page p = ctx.cacheManager.fetchAndPin(((InsertLogAction) la).getPageNo());
                    System.arraycopy(emptyData, 0, p.data, 0, emptyData.length);
                    p.dirty = true;
                    p.empty = false;
                    ctx.cacheManager.unpin(p.pageNo);
                    break;
                }
                case LogAction.UPDATE: {
                    Page p = ((UpdateLogAction) la).getAfterImage();
                    ctx.cacheManager.fetchAndPin(p.pageNo);
                    p.dirty = true;
                    ctx.cacheManager.replace(p);
                    ctx.cacheManager.unpin(p.pageNo);
                    break;
                }
                case LogAction.DELETE: {
                    int pageNo = ((DeleteLogAction) la).getPageNo();
                    Page p = ctx.cacheManager.fetchAndPin(pageNo);
                    p.dirty = true;
                    p.empty = true;
                    ctx.cacheManager.unpin(p.pageNo);
                    break;
                }
                case LogAction.UPDATE_PORTION: {
                    UpdatePortionLogAction upla = (UpdatePortionLogAction) la;
                    Page p = ctx.cacheManager.fetchAndPin(upla.getPageNo());
                    p.dirty = true;
                    p.empty = false;
                    byte[] ai = upla.getAfterImage();
                    System.arraycopy(ai, 0, p.data, upla.getOffset(), ai.length);
                    ctx.cacheManager.unpin(p.pageNo);
                    break;
                }
            }
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/processCommit...done.");
    }

    private void processAbort(List journal) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/processAbort...");

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/processAbort, " + journal.size() + " actions aborted");
        rollbacks++;
        for (int i = journal.size() - 1; i >= 0; i--) {
            LogAction la = (LogAction) journal.get(i);
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/processAbort, " + la);
            switch (la.getType()) {
                case LogAction.INSERT: {
                    Page p = ctx.cacheManager.fetchAndPin(((InsertLogAction) la).getPageNo());
                    p.dirty = true;
                    p.empty = true;
                    ctx.cacheManager.unpin(p.pageNo);
                    break;
                }
                case LogAction.UPDATE: {
                    Page p = ((UpdateLogAction) la).getBeforeImage();
                    ctx.cacheManager.fetchAndPin(p.pageNo);
                    p.dirty = true;
                    ctx.cacheManager.replace(p);
                    ctx.cacheManager.unpin(p.pageNo);
                    break;
                }
                case LogAction.DELETE: {
                    int pageNo = ((DeleteLogAction) la).getPageNo();
                    byte[] bi = ((DeleteLogAction) la).getBeforeImage();
                    Page p = ctx.cacheManager.fetchAndPin(pageNo);
                    p.dirty = true;
                    System.arraycopy(bi, 0, p.data, 0, bi.length);
                    ctx.cacheManager.unpin(p.pageNo);
                    break;
                }
                case LogAction.UPDATE_PORTION: {
                    UpdatePortionLogAction upla = (UpdatePortionLogAction) la;
                    if (upla.getBeforeImage() != null) {
                        Page p = ctx.cacheManager.fetchAndPin(upla.getPageNo());
                        p.dirty = true;
                        p.empty = false;
                        byte[] bi = upla.getBeforeImage();
                        System.arraycopy(bi, 0, p.data, upla.getOffset(), bi.length);
                        ctx.cacheManager.unpin(p.pageNo);
                    }
                    break;
                }
            }
        }

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/processAbort...done.");
    }

    private boolean processLogRecord(LogRecord lr, boolean first) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/processLogRecord: " + lr);
        if (first && lr.getLogType() == LogRecord.START) {
            magic = lr.getMagic();
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/processLogRecord...done.");
            return true;
        }
        if (magic != -1 && lr.getMagic() != magic) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", toString() + "/processLogRecord, magic doesn't match, end of log");
            return false;
        }
        if (!lr.isComplete()) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", toString() + "/processLogRecord, log record isn't complete!");
        }
        List journal = lr.getJournal();
        if (journal != null && journal.size() > 0) {
            switch (lr.getLogType()) {
                case LogRecord.COMMIT:
                    processCommit(journal);
                    if (!lr.isComplete())
                        processAbort(journal);
                    break;
                case LogRecord.ABORT:
                    processCommit(journal);
                    processAbort(journal);
                    break;
            }
        } else if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/processLogRecord, journal is empty!");
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/processLogRecord...done.");
        return true;
    }

    public void restart(boolean recover) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/restart...");
        commits = 0;
        rollbacks = 0;
        prepares = 0;
        boolean ccheck = new Boolean(System.getProperty(PROP_ANALYZE, "false")).booleanValue();

        // Restart
        RandomAccessFile logFile = ctx.logManager.getLogFile();
        logFile.seek(0);
        if (logFile.length() > 0) {
            long bucket = logFile.length() / 10;
            long lastPercent = 0;
            ctx.cacheManager.setForceEnsure(true);
            LogRecord lr = null;
            boolean first = true;
            boolean startLogRecOnly = false;
            int nLogRecs = 0;
            boolean startMsgPrinted = false;
            try {
                for (; ; ) {
                    try {
                        lr = LogRecord.create(logFile.readInt());
                        lr.readContent(logFile, magic != -1);
                        if (!processLogRecord(lr, first))
                            break;
                        first = false;
                        nLogRecs++;
                        if (!startMsgPrinted && (magic == -1 && nLogRecs == 1 || nLogRecs == 2)) {
                            System.out.println("+++ RecoveryManager/restarting, processing transaction log...");
                            startMsgPrinted = true;
                        }
                        startLogRecOnly = lr.getLogType() == LogRecord.START;
                    } catch (Exception e) {
                        // Exception means it is not possible anymore to construct a log record
                        if (startMsgPrinted)
                            System.out.println("+++ RecoveryManager/restart, end of log detected.");
                        break;
                    }
                    lr = null;
                    if (logFile.getFilePointer() / bucket > lastPercent) {
                        if (startMsgPrinted)
                            System.out.println("+++ RecoveryManager/restart, " + (logFile.getFilePointer() / bucket) + "0% so far...");
                        lastPercent = logFile.getFilePointer() / bucket;
                        ctx.cacheManager.flush();
                    }
                }
            } catch (IOException ioe) {
                // OK, End of log, process the last one
                if (lr != null)
                    processLogRecord(lr, first);
            }
            if (!ccheck)
                ccheck = !startLogRecOnly;
            if (startMsgPrinted)
                System.out.println("+++ RecoveryManager/restart done.");
            ctx.cacheManager.setForceEnsure(false);
            ctx.cacheManager.flush();
            ctx.cacheManager.reset();
            logFile.seek(0);
            System.gc();
        }

        IndexAnalyzer analyzer = new IndexAnalyzer(ctx);

        // Consistency check ...
        if (ccheck) {
            System.out.println("+++ consistency check in progress ...");
            analyzer.checkConsistency(recover);
            System.out.println("+++ consistency check done.");
        }

        // Analyze...
        String s = System.getProperty(PROP_ANALYZE);
        if (s != null && s.equals("true")) {
            System.out.println("+++ store analysis in progress ...");
            analyzer.analyzeRootIndex(new PrintWriter(new FileWriter("rootindex.analyze"), true));
            System.out.println("+++ store analysis done.");
        }

        // build reference map
        analyzer.buildReferences();

        ctx.logSwiftlet.logInformation("sys$store", toString() + "/restart, " + commits + " transactions rolled forward");
        ctx.logSwiftlet.logInformation("sys$store", toString() + "/restart, " + rollbacks + " transactions aborted");

        // Start LogManager
        ctx.logManager.startQueue();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/restart...done.");
    }

    public void commit(CommitLogRecord logRecord) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/commit...");
        ctx.logManager.enqueue(logRecord);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/commit...done.");
    }

    public void abort(AbortLogRecord logRecord) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/abort...");
        synchronized (this) {
            processAbort(logRecord.getJournal());
        }
        ctx.logManager.enqueue(logRecord);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/abort...done.");
    }

    public String toString() {
        return "RecoveryManager";
    }
}

