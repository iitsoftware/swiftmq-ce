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
import com.swiftmq.impl.store.standard.backup.po.ChangeGenerations;
import com.swiftmq.impl.store.standard.backup.po.ChangePath;
import com.swiftmq.impl.store.standard.backup.po.ScanSaveSets;
import com.swiftmq.impl.store.standard.backup.po.StartBackup;
import com.swiftmq.impl.store.standard.cache.CacheManager;
import com.swiftmq.impl.store.standard.cache.StableStore;
import com.swiftmq.impl.store.standard.index.QueueIndex;
import com.swiftmq.impl.store.standard.index.RootIndex;
import com.swiftmq.impl.store.standard.jobs.JobRegistrar;
import com.swiftmq.impl.store.standard.log.*;
import com.swiftmq.impl.store.standard.pagedb.StoreConverter;
import com.swiftmq.impl.store.standard.pagedb.scan.ScanProcessor;
import com.swiftmq.impl.store.standard.pagedb.scan.po.StartScan;
import com.swiftmq.impl.store.standard.pagedb.shrink.ShrinkProcessor;
import com.swiftmq.impl.store.standard.pagedb.shrink.po.StartShrink;
import com.swiftmq.impl.store.standard.recover.RecoveryManager;
import com.swiftmq.impl.store.standard.swap.SwapFileFactory;
import com.swiftmq.impl.store.standard.swap.SwapFileFactoryImpl;
import com.swiftmq.impl.store.standard.transaction.TransactionManager;
import com.swiftmq.impl.store.standard.xa.PrepareLogRecordImpl;
import com.swiftmq.impl.store.standard.xa.PreparedLogQueue;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.event.SwiftletManagerAdapter;
import com.swiftmq.swiftlet.event.SwiftletManagerEvent;
import com.swiftmq.swiftlet.scheduler.SchedulerSwiftlet;
import com.swiftmq.swiftlet.store.*;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.util.SwiftUtilities;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

public class StoreSwiftletImpl extends StoreSwiftlet {
    private static final String PREPARED_LOG_QUEUE = "sys$prepared";
    private static boolean PRECREATE = Boolean.valueOf(System.getProperty("swiftmq.store.txlog.precreate", "true")).booleanValue();
    StoreContext ctx = null;
    RootIndex rootIndex = null;
    long swapMaxLength = 0;
    JobRegistrar jobRegistrar = null;

    public synchronized PersistentStore getPersistentStore(String queueName)
            throws StoreException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", "getPersistentStore, queueName=" + queueName);
        QueueIndex queueIndex = null;
        try {
            queueIndex = rootIndex.getQueueIndex(queueName);
        } catch (Exception e) {
            throw new StoreException(e.getMessage());
        }
        PersistentStore ps = new PersistentStoreImpl(ctx, queueIndex, rootIndex, queueName);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", "getPersistentStore, queueName=" + queueName + ", ps=" + ps);
        return ps;
    }

    public synchronized NonPersistentStore getNonPersistentStore(String queueName)
            throws StoreException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", "getNonPersistentStore, queueName=" + queueName);
        NonPersistentStore nps = createNonPersistentStore(ctx, queueName, ctx.swapPath, swapMaxLength);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", "getNonPersistentStore, queueName=" + queueName + ", nps=" + nps);
        return nps;
    }

    public synchronized DurableSubscriberStore getDurableSubscriberStore()
            throws StoreException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", "getDurableSubscriberStore...");
        DurableSubscriberStore ds = ctx.durableStore;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", "getDurableSubscriberStore, ds=" + ds);
        return (ds);
    }

    public synchronized List getPrepareLogRecords() throws StoreException {
        List list = null;
        try {
            list = ctx.preparedLog.getAll();
        } catch (IOException e) {
            throw new StoreException(e.toString());
        }
        return list;
    }

    public synchronized void removePrepareLogRecord(PrepareLogRecord record) throws StoreException {
        try {
            ctx.preparedLog.remove((PrepareLogRecordImpl) record);
        } catch (IOException e) {
            throw new StoreException(e.toString());
        }
    }

    public CompositeStoreTransaction createCompositeStoreTransaction() {
        CompositeStoreTransaction tx = new CompositeStoreTransactionImpl(ctx);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", "createCompositeStoreTransaction, tx=" + tx);
        return tx;
    }

    public void flushCache() {
        try {
            ctx.cacheManager.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void deleteSwaps() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", "deleteSwaps...");
        new File(ctx.swapPath).mkdirs();
        File[] list = new File(ctx.swapPath).listFiles();
        for (int i = 0; i < list.length; i++) {
            if (list[i].getName().endsWith(".swap"))
                list[i].delete();
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", "deleteSwaps...done");
    }

    private void checkBackupPath() throws SwiftletException {
        Property prop = ctx.backupEntity.getProperty("path");
        String path = SwiftUtilities.addWorkingDir((String) prop.getValue());
        File f = new File(path);
        if (f.exists()) {
            if (!f.isDirectory())
                throw new SwiftletException("Invalid Backup Path (not a Directory): " + path + "(absolute paths must be prefixed with \"absolute:\")");
        } else {
            if (!f.mkdirs())
                throw new SwiftletException("Invalid Backup Path (unable to create Directory): " + path + "(absolute paths must be prefixed with \"absolute:\")");
        }
        ctx.backupProcessor.enqueue(new ScanSaveSets());
        prop.setPropertyChangeListener(new PropertyChangeListener() {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                String s = SwiftUtilities.addWorkingDir((String) newValue);
                File file = new File(s);
                if (file.exists()) {
                    if (!file.isDirectory())
                        throw new PropertyChangeException("Invalid Backup Path (not a Directory): " + s + "(absolute paths must be prefixed with \"absolute:\")");
                } else {
                    if (!file.mkdirs())
                        throw new PropertyChangeException("Invalid Backup Path (unable to create Directory): " + s + "(absolute paths must be prefixed with \"absolute:\")");
                }
                Semaphore sem = new Semaphore();
                ChangePath po = new ChangePath(sem, (String) newValue);
                ctx.backupProcessor.enqueue(po);
                sem.waitHere();
                if (!po.isSuccess())
                    throw new PropertyChangeException(po.getException());
                ctx.backupProcessor.enqueue(new ScanSaveSets());
            }
        });
        prop = ctx.backupEntity.getProperty("keep-generations");
        prop.setPropertyChangeListener(new PropertyChangeListener() {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                int i = ((Integer) newValue).intValue();
                Semaphore sem = new Semaphore();
                ChangeGenerations po = new ChangeGenerations(sem, i);
                ctx.backupProcessor.enqueue(po);
                sem.waitHere();
                if (!po.isSuccess())
                    throw new PropertyChangeException(po.getException());
                ctx.backupProcessor.enqueue(new ScanSaveSets());
            }
        });
        CommandRegistry commandRegistry = ctx.backupEntity.getCommandRegistry();
        CommandExecutor backupExecutor = new CommandExecutor() {
            public String[] execute(String[] context, Entity entity, String[] cmd) {
                if (cmd.length != 1)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'backup'"};
                Semaphore sem = new Semaphore();
                StartBackup po = new StartBackup(sem, null);
                ctx.backupProcessor.enqueue(po);
                sem.waitHere();
                String[] result = null;
                if (po.isSuccess())
                    result = new String[]{TreeCommands.INFO, "Backup initiated. Please watch Folder 'Generated Backup Save Sets'."};
                else
                    result = new String[]{TreeCommands.ERROR, po.getException()};
                return result;
            }
        };
        Command backupCommand = new Command("backup", "backup", "Perform Backup Now", true, backupExecutor, true, false);
        commandRegistry.addCommand(backupCommand);
    }

    protected NonPersistentStore createNonPersistentStore(StoreContext ctx, String queueName, String swapPath, long swapMaxLength) {
        return new NonPersistentStoreImpl(ctx, queueName, swapPath, swapMaxLength);
    }

    protected StableStore createStableStore(StoreContext ctx, String path, int initialSize) throws Exception {
        return new StableStore(ctx, path, initialSize);
    }

    protected DurableSubscriberStoreImpl createDurableSubscriberStore(StoreContext ctx, String path) throws StoreException {
        return new DurableSubscriberStoreImpl(ctx, path);
    }

    public LogFile createTxLogFile(String filename, String mode) throws Exception {
        new File(filename).getParentFile().mkdirs();
        RandomAccessFile file = new RandomAccessFile(filename, mode);
        LogFile logFile = null;
        if (PRECREATE)
            logFile = new ReuseLogFile(ctx, file);
        else
            logFile = new AppendLogFile(ctx, file);
        return logFile;
    }

    protected SwapFileFactory createSwapFileFactory() {
        return new SwapFileFactoryImpl(ctx);
    }

    protected LogManagerFactory createLogManagerFactory() {
        return new LogManagerFactoryImpl();
    }

    public void startStore(boolean withRecovery) throws Exception {
        ctx.recoveryManager = new RecoveryManager(ctx);
        ctx.stableStore = createStableStore(ctx, SwiftUtilities.addWorkingDir((String) ctx.dbEntity.getProperty("path").getValue()),
                (Integer) ctx.dbEntity.getProperty("initial-db-size-pages").getValue());
        Property minCacheSizeProp = ctx.cacheEntity.getProperty("min-size");
        Property maxCacheSizeProp = ctx.cacheEntity.getProperty("max-size");
        int minCacheSize = (Integer) minCacheSizeProp.getValue();
        int maxCacheSize = (Integer) maxCacheSizeProp.getValue();
        if (minCacheSize > maxCacheSize)
            throw new Exception("Cache/min-size is invalid, must be less than max-size!");
        minCacheSizeProp.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                int n = (Integer) newValue;
                if (n > (Integer) ctx.cacheEntity.getProperty("max-size").getValue())
                    throw new PropertyChangeException("min-size is invalid, must be less than max-size!");
            }
        });
        maxCacheSizeProp.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                int n = (Integer) newValue;
                if (n < (Integer) ctx.cacheEntity.getProperty("min-size").getValue())
                    throw new PropertyChangeException("max-size is invalid, must be greater than min-size!");
            }
        });

        ctx.cacheManager = new CacheManager(ctx, ctx.stableStore, minCacheSize, maxCacheSize);
        ctx.transactionManager = new TransactionManager(ctx);
        ctx.logManager = createLogManagerFactory().createLogManager(ctx, ctx.transactionManager,
                SwiftUtilities.addWorkingDir((String) ctx.txEntity.getProperty("path").getValue()),
                (Long) ctx.txEntity.getProperty("checkpoint-size").getValue(),
                (Boolean) ctx.txEntity.getProperty("force-sync").getValue());
        ctx.recoveryManager.restart(withRecovery);
    }

    public void stopStore() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", "shutdown, stopping log manager...");
        Semaphore sem = new Semaphore();
        ctx.logManager.enqueue(new CloseLogOperation(sem));
        sem.waitHere();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", "shutdown, stopping log manager...done");
        ctx.logManager.stopQueue();
        ctx.cacheManager.flush();
        ctx.cacheManager.close();
    }

    protected void startup(Configuration config)
            throws SwiftletException {
        try {
            ctx = new StoreContext(this, config);
            ctx.storeConverter = new StoreConverter(ctx, SwiftUtilities.addWorkingDir((String) ctx.dbEntity.getProperty("path").getValue()));

            ctx.swapFileFactory = createSwapFileFactory();
            ctx.swapPath = SwiftUtilities.addWorkingDir((String) ctx.swapEntity.getProperty("path").getValue());
            swapMaxLength = (Long) ctx.swapEntity.getProperty("roll-over-size").getValue();

            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", "startup...");

            ctx.durableStore = createDurableSubscriberStore(ctx, SwiftUtilities.addWorkingDir((String) ctx.durableEntity.getProperty("path").getValue()));

            deleteSwaps();

            // Required for recovery of transactions
            startStore(true);
            stopStore();

            // Phase 1: Store/Cache inactive
            ctx.storeConverter.phaseOne();

            startStore(false);

            // Phase 2: Store & Cache are active with the new Page Size
            ctx.storeConverter.phaseTwo();

            rootIndex = new RootIndex(ctx, 0);
            ctx.preparedLog = new PreparedLogQueue(ctx, rootIndex.getQueueIndex(PREPARED_LOG_QUEUE));
            ctx.backupProcessor = new BackupProcessor(ctx);
            checkBackupPath();
            ctx.shrinkProcessor = new ShrinkProcessor(ctx);
            CommandRegistry commandRegistry = ctx.dbEntity.getCommandRegistry();
            CommandExecutor shrinkExecutor = (context, entity, cmd) -> {
                if (cmd.length != 1)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'shrink'"};
                Semaphore sem = new Semaphore();
                StartShrink po = new StartShrink(sem);
                ctx.shrinkProcessor.enqueue(po);
                sem.waitHere();
                String[] result = null;
                if (po.isSuccess())
                    result = new String[]{TreeCommands.INFO, "Shrink initiated."};
                else
                    result = new String[]{TreeCommands.ERROR, po.getException()};
                return result;
            };
            Command shrinkCommand = new Command("shrink", "shrink", "Perform Shrink Now", true, shrinkExecutor, true, false);
            commandRegistry.addCommand(shrinkCommand);

            ctx.scanProcessor = new ScanProcessor(ctx);
            CommandExecutor scanExecutor = (context, entity, cmd) -> {
                if (cmd.length != 1)
                    return new String[]{TreeCommands.ERROR, "Invalid command, please try 'scan'"};
                Semaphore sem = new Semaphore();
                StartScan po = new StartScan(sem);
                ctx.scanProcessor.enqueue(po);
                sem.waitHere();
                String[] result = null;
                if (po.isSuccess())
                    result = new String[]{TreeCommands.INFO, "Scanning page.db initiated."};
                else
                    result = new String[]{TreeCommands.ERROR, po.getException()};
                return result;
            };
            Command scanCommand = new Command("scan", "scan", "Scan page.db", true, scanExecutor, true, false);
            commandRegistry.addCommand(scanCommand);

            SwiftletManager.getInstance().addSwiftletManagerListener("sys$scheduler", new SwiftletManagerAdapter() {
                public void swiftletStarted(SwiftletManagerEvent event) {
                    ctx.schedulerSwiftlet = (SchedulerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$scheduler");
                    jobRegistrar = new JobRegistrar(ctx);
                    jobRegistrar.register();
                }

                public void swiftletStopInitiated(SwiftletManagerEvent event) {
                    jobRegistrar.unregister();
                }
            });

            // Ensure to save the new current page size
            if (ctx.storeConverter.isConverted()) {
                SwiftletManager.getInstance().setConfigDirty(true);
            }

            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", "startup...done");
        } catch (Exception e) {
            e.printStackTrace();
            throw new SwiftletException(e.getMessage());
        }
    }

    /**
     * Shutdown the swiftlet. Check if all shutdown conditions are met. Do shutdown work (i. e. stop working thread, close resources).
     * If any condition prevends from shutdown fire a SwiftletException.
     *
     * @throws com.swiftmq.swiftlet.SwiftletException
     */
    protected void shutdown()
            throws SwiftletException {
        // true if shutdown while standby
        if (ctx == null)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", "shutdown...");
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", "shutdown, stopping scan processor...");
            ctx.scanProcessor.close();
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", "shutdown, stopping shrink processor...");
            ctx.shrinkProcessor.close();
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", "shutdown, stopping backup processor...");
            ctx.backupProcessor.close();
            stopStore();
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", "shutdown...done");
        ctx = null;
    }
}
