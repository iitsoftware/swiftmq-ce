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

import com.swiftmq.impl.store.standard.backup.BackupProcessor;
import com.swiftmq.impl.store.standard.cache.CacheManager;
import com.swiftmq.impl.store.standard.cache.StableStore;
import com.swiftmq.impl.store.standard.index.ReferenceMap;
import com.swiftmq.impl.store.standard.log.LogManager;
import com.swiftmq.impl.store.standard.pagedb.PageSize;
import com.swiftmq.impl.store.standard.pagedb.StoreConverter;
import com.swiftmq.impl.store.standard.pagedb.scan.ScanProcessor;
import com.swiftmq.impl.store.standard.pagedb.shrink.ShrinkProcessor;
import com.swiftmq.impl.store.standard.recover.RecoveryManager;
import com.swiftmq.impl.store.standard.swap.SwapFileFactory;
import com.swiftmq.impl.store.standard.transaction.TransactionManager;
import com.swiftmq.impl.store.standard.xa.PreparedLog;
import com.swiftmq.mgmt.Configuration;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.scheduler.SchedulerSwiftlet;
import com.swiftmq.swiftlet.threadpool.EventLoop;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;

public class StoreContext {
    public CacheManager cacheManager = null;
    public StableStore stableStore = null;
    public PreparedLog preparedLog = null;
    public DurableSubscriberStoreImpl durableStore = null;
    public LogManager logManager = null;
    public EventLoop logManagerEventLoop = null;
    public TransactionManager transactionManager = null;
    public RecoveryManager recoveryManager = null;
    public BackupProcessor backupProcessor = null;
    public ShrinkProcessor shrinkProcessor = null;
    public ScanProcessor scanProcessor = null;
    public TimerSwiftlet timerSwiftlet = null;
    public SchedulerSwiftlet schedulerSwiftlet = null;
    public TraceSwiftlet traceSwiftlet = null;
    public TraceSpace traceSpace = null;
    public LogSwiftlet logSwiftlet = null;
    public ThreadpoolSwiftlet threadpoolSwiftlet = null;
    public StoreSwiftletImpl storeSwiftlet = null;
    public Configuration config = null;
    public Entity backupEntity = null;
    public Entity txEntity = null;
    public Entity dbEntity = null;
    public Entity cacheEntity = null;
    public Entity swapEntity = null;
    public Entity durableEntity = null;
    public EntityList backupList = null;
    public EntityList filesList = null;
    public EntityList scanList = null;
    public SwapFileFactory swapFileFactory = null;
    public String swapPath = null;
    public ReferenceMap referenceMap = null;
    public StoreConverter storeConverter = null;

    public StoreContext(StoreSwiftletImpl storeSwiftlet, Configuration config) {
        this.storeSwiftlet = storeSwiftlet;
        this.config = config;
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
        backupEntity = config.getEntity("backup");
        logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
        timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");
        threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
        txEntity = config.getEntity("transaction-log");
        dbEntity = config.getEntity("database");
        cacheEntity = config.getEntity("cache");
        durableEntity = config.getEntity("durable-subscriber");
        swapEntity = config.getEntity("swap");
        backupList = (EntityList) config.getEntity("usage").getEntity("backup");
        filesList = (EntityList) config.getEntity("usage").getEntity("files");
        scanList = (EntityList) config.getEntity("usage").getEntity("scan");
        referenceMap = new ReferenceMap(this);
        PageSize.init(dbEntity);
    }

    public StoreContext() {
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
        logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
    }
}

