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
import com.swiftmq.impl.store.standard.cache.CacheReleaseListener;
import com.swiftmq.impl.store.standard.index.MessagePageReference;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.threadpool.EventProcessor;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;
import com.swiftmq.tools.concurrent.Semaphore;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class LogManagerEventProcessor implements LogManager, EventProcessor {
    StoreContext ctx;
    CheckPointHandler checkPointHandler;
    LogManagerListener logManagerListener = null;
    String path;
    LogFile logFile;
    long maxLogSize;
    boolean forceSync = false;
    boolean checkPointInitiated = false;
    boolean checkPointPending = false;
    List<Semaphore> semList = null;
    List<CacheReleaseListener> releaseList = null;
    List<AsyncCompletionCallback> callbackList = null;
    Semaphore closeSem = null;
    boolean closed = false;

    public LogManagerEventProcessor(StoreContext ctx, CheckPointHandler checkPointHandler, String path, long maxLogSize, boolean forceSync) throws Exception {
        this.ctx = ctx;
        this.checkPointHandler = checkPointHandler;
        this.path = path;
        this.maxLogSize = maxLogSize;
        this.forceSync = forceSync;
        logFile = ctx.storeSwiftlet.createTxLogFile(path + File.separatorChar + FILENAME, "rw");
        logFile.init(maxLogSize + (maxLogSize / 4));
        semList = new ArrayList<>();
        releaseList = new ArrayList<>();
        callbackList = new ArrayList<>();
        ctx.logSwiftlet.logInformation("sys$store", this + "/create, maxLogSize=" + maxLogSize + ", file=" + path + File.separatorChar + FILENAME + ", logFile=" + logFile);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", this + "/create, maxLogSize=" + maxLogSize + ", file=" + path + File.separatorChar + FILENAME + ", logFile=" + logFile);
    }

    public void setLogManagerListener(LogManagerListener logManagerListener) {
        this.logManagerListener = logManagerListener;
    }

    public RandomAccessFile getLogFile() {
        return logFile.getFile();
    }

    public void setForceSync(boolean forceSync) {
        this.forceSync = forceSync;
    }

    public boolean isForceSync() {
        return forceSync;
    }

    public void process(List<Object> events) {
        if (closed && !checkPointPending)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", this + "/process,length=" + events.size());
        try {
            if (logManagerListener != null)
                logManagerListener.startProcessing();
            boolean checkPointNow = false;
            for (Object event : events) {
                LogOperation oper = (LogOperation) event;
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$store", this + "/process, operation=" + oper);
                switch (oper.getOperationType()) {
                    case LogOperation.OPER_LOG_REC:
                        LogRecord lr = (LogRecord) oper;
                        logFile.write(lr);
                        Semaphore s = lr.getSemaphore();
                        if (s != null)
                            semList.add(s);
                        CacheReleaseListener crl = lr.getCacheReleaseListener();
                        if (crl != null)
                            releaseList.add(crl);
                        AsyncCompletionCallback cb = lr.getCallback();
                        if (cb != null)
                            callbackList.add(cb);
                        // Releasing locks on shared message pages
                        List<MessagePageReference> messagePageRefs = lr.getMessagePageRefs();
                        if (messagePageRefs != null) {
                            for (MessagePageReference messagePageRef : messagePageRefs) {
                                messagePageRef.unMarkActive();
                            }
                        }
                        break;
                    case LogOperation.OPER_CLOSE_LOG:
                        closeSem = ((CloseLogOperation) oper).getSemaphore();
                        closed = true;
                        break;
                    case LogOperation.OPER_INITIATE_SYNC:
                        checkPointInitiated = true;
                        break;
                    case LogOperation.OPER_SYNC_LOG:
                        checkPointNow = true;
                        break;
                }
            }

            // write the log to the log file
            if (logFile.getFlushSize() > 0)
                logFile.flush(forceSync);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", this + "/process, processed log objects=" + events.size());

            if (checkPointNow) {
                if (checkPointVerbose)
                    System.out.println("+++ CHECKPOINT ... ");
                long start = System.currentTimeMillis();
                // Trigger CP Handler to flush the cache.
                checkPointHandler.performCheckPoint();
                // Start new log file
                logFile.reset(forceSync);
                // continue normal operation
                checkPointHandler.checkPointDone();
                checkPointPending = false;
                if (checkPointVerbose)
                    System.out.println("+++ CHECKPOINT DONE in " + (System.currentTimeMillis() - start) + " milliseconds");
                if (closed) {
                    if (logManagerListener != null)
                        logManagerListener.stopProcessing();
                    closeSem.notifySingleWaiter();
                    return;
                }
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", this + "/process, releaseList.size=" + releaseList.size() + ", semList.size=" + semList.size() + ", callbackList.size=" + callbackList.size());

            // call back cache release listeners
            if (!releaseList.isEmpty()) {
                for (CacheReleaseListener cacheReleaseListener : releaseList) {
                    cacheReleaseListener.releaseCache();
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$store", this + "/process, " + releaseList.size() + " CacheReleaseListeners called");
                releaseList.clear();
            }

            if (logManagerListener != null)
                logManagerListener.stopProcessing();

            // notify Semaphores
            if (!semList.isEmpty()) {
                for (Semaphore ls : semList) {
                    ls.notifySingleWaiter();
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$store", this + "/process, " + semList.size() + " semaphores notified");
                semList.clear();
            }

            // notify callbacks
            if (!callbackList.isEmpty()) {
                for (AsyncCompletionCallback asyncCompletionCallback : callbackList) {
                    AsyncCompletionCallback cb = (AsyncCompletionCallback) asyncCompletionCallback;
                    cb.notifyCallbackStack(true);
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$store", this + "/process, " + callbackList.size() + " AsyncCompletionCallbacks called");
                callbackList.clear();
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", this + "/process, logFile.length=" + logFile.getPosition());

            // Inform CP Handler that a checkpoint is required.
            // The CP Handler locks the system and puts a
            // SyncLogOperation into the queue.
            if (!checkPointPending && (closed || checkPointInitiated || logFile.getPosition() >= maxLogSize)) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$store", this + "/process, checkPointHandler.lockForCheckPoint()");
                checkPointHandler.lockForCheckPoint();
                checkPointPending = true;
                checkPointInitiated = false;
            }
        } catch (Exception e) {
            // PANIC
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", this + "/process, exception occurred=" + e);
            ctx.logSwiftlet.logError("sys$store", this + "/process, PANIC! EXITING! Exception occurred=" + e);
            e.printStackTrace();
            SwiftletManager.getInstance().disableShutdownHook();
            System.exit(-1);
        }
    }

    public String toString() {
        return "LogManagerEventProcessor";
    }
}

