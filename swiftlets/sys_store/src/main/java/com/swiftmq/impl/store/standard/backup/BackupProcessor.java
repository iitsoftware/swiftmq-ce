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

package com.swiftmq.impl.store.standard.backup;

import com.swiftmq.impl.store.standard.StoreContext;
import com.swiftmq.impl.store.standard.backup.po.*;
import com.swiftmq.impl.store.standard.log.CheckPointFinishedListener;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityAddException;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.EntityRemoveException;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.PipelineQueue;
import com.swiftmq.util.SwiftUtilities;

import java.io.File;
import java.io.FilenameFilter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

public class BackupProcessor implements EventVisitor, CheckPointFinishedListener {
    static final String TP_BACKUP = "sys$store.backup";
    static final String COMPLETED_FILE = ".completed";
    static final String SAVESET = "saveset_";
    static final SimpleDateFormat fmt = new SimpleDateFormat("'" + SAVESET + "'yyyyMMddHHmmssSSS");
    StoreContext ctx = null;
    String path = null;
    int generations = 0;
    PipelineQueue pipelineQueue = null;
    boolean backupActive = false;
    String currentSaveSet = null;
    BackupFinishedListener finishedListener = null;

    public BackupProcessor(StoreContext ctx) {
        this.ctx = ctx;
        path = SwiftUtilities.addWorkingDir((String) ctx.backupEntity.getProperty("path").getValue());
        generations = ((Integer) ctx.backupEntity.getProperty("keep-generations").getValue()).intValue();
        pipelineQueue = new PipelineQueue(ctx.threadpoolSwiftlet.getPool(TP_BACKUP), TP_BACKUP, this);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/created");
    }

    // Called from the LogManager after a Checkpoint has been performed and before the Transaction Manager
    // is restarted
    public void checkpointFinished() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/checkpointFinished ...");
        BackupCompleted nextPO = new BackupCompleted();
        try {
            if (ctx.preparedLog.backupRequired())
                ctx.preparedLog.backup(currentSaveSet);
            ctx.stableStore.copy(currentSaveSet);
            ctx.durableStore.copy(currentSaveSet);
            new File(currentSaveSet + File.separatorChar + COMPLETED_FILE).createNewFile();
            nextPO.setSuccess(true);
        } catch (Exception e) {
            nextPO.setSuccess(false);
            nextPO.setException(e.toString());
        }
        enqueue(new ScanSaveSets(nextPO));
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/checkpointFinished done");
    }

    public void enqueue(POObject po) {
        pipelineQueue.enqueue(po);
    }

    private void deleteSaveSet(File dir) {
        ctx.logSwiftlet.logInformation(ctx.storeSwiftlet.getName(), toString() + "/Delete save set: " + dir.getName());
        File[] files = dir.listFiles();
        if (files != null) {
            for (int i = 0; i < files.length; i++)
                files[i].delete();
        }
        dir.delete();
    }

    private void scanSaveSet(File dir) {
        File completed = new File(dir, COMPLETED_FILE);
        if (completed.exists()) {
            // Create Usage Entry
            Entity entity = ctx.backupList.createEntity();
            entity.setName(dir.getName());
            entity.createCommands();
            try {
                ctx.backupList.addEntity(entity);
            } catch (EntityAddException e) {
            }
            EntityList fileList = (EntityList) entity.getEntity("files");
            File[] files = dir.listFiles(new FilenameFilter() {
                public boolean accept(File d, String name) {
                    return !name.equals(COMPLETED_FILE);
                }
            });
            if (files != null) {
                for (int i = 0; i < files.length; i++) {
                    Entity fileEntity = fileList.createEntity();
                    fileEntity.setName(files[i].getName());
                    fileEntity.createCommands();
                    try {
                        fileEntity.getProperty("filesize").setValue(Long.valueOf(files[i].length()));
                        fileList.addEntity(fileEntity);
                    } catch (Exception e) {
                    }
                }
            }
        } else {
            // Delete Folder
            deleteSaveSet(dir);
        }
    }

    public void visit(ScanSaveSets po) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/" + po + " ...");

        // Delete Usage content
        Map entities = ctx.backupList.getEntities();
        if (entities != null) {
            for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
                try {
                    ctx.backupList.removeEntity((Entity) ((Map.Entry) iter.next()).getValue());
                } catch (EntityRemoveException e) {
                }
            }
        }

        // Scan save sets
        File dir = new File(path);
        File[] files = dir.listFiles(new FilenameFilter() {
            public boolean accept(File d, String name) {
                return name.startsWith(SAVESET) && d.isDirectory();
            }
        });
        if (files != null) {
            for (int i = 0; i < files.length; i++) {
                scanSaveSet(files[i]);
            }
        }

        // Check Number Generation and eventually delete the oldest Generation
        String[] names = ctx.backupList.getEntityNames();
        if (names != null && names.length > generations) {
            Arrays.sort(names);
            int toRemove = names.length - generations;
            for (int i = 0; i < toRemove; i++) {
                try {
                    ctx.backupList.removeEntity(ctx.backupList.getEntity(names[i]));
                    deleteSaveSet(new File(path + File.separatorChar + names[i]));
                } catch (EntityRemoveException e) {
                }
            }
        }

        // Enqueue next PO
        if (po.getNextPO() != null)
            enqueue(po.getNextPO());
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(ChangePath po) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/" + po + " ...");
        if (backupActive) {
            // reject it
            po.setException("Can't change Path: another Backup is active right now!");
            po.setSuccess(false);
        } else {
            po.setSuccess(true);
            path = SwiftUtilities.addWorkingDir(po.getNewPath());
        }
        Semaphore sem = po.getSemaphore();
        if (sem != null)
            sem.notifySingleWaiter();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(ChangeGenerations po) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/" + po + " ...");
        if (backupActive) {
            // reject it
            po.setException("Can't change Generations: another Backup is active right now!");
            po.setSuccess(false);
        } else {
            po.setSuccess(true);
            generations = po.getNewGenerations();
        }
        Semaphore sem = po.getSemaphore();
        if (sem != null)
            sem.notifySingleWaiter();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(StartBackup po) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/" + po + " ...");
        if (backupActive) {
            // reject it
            String msg = "Can't start Backup: another Backup is active right now!";
            po.setException(msg);
            po.setSuccess(false);
            BackupFinishedListener l = po.getFinishedListener();
            if (l != null)
                l.backupFinished(false, msg);
            ctx.logSwiftlet.logError(ctx.storeSwiftlet.getName(), toString() + "/" + msg);
        } else {
            // start backup
            backupActive = true;
            currentSaveSet = path + File.separatorChar + fmt.format(new Date());
            ctx.logSwiftlet.logInformation(ctx.storeSwiftlet.getName(), toString() + "/Backup started, save set: " + currentSaveSet);
            new File(currentSaveSet).mkdir();
            finishedListener = po.getFinishedListener();
            po.setSuccess(true);
            ctx.transactionManager.initiateCheckPoint(this);
        }
        Semaphore sem = po.getSemaphore();
        if (sem != null)
            sem.notifySingleWaiter();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(BackupCompleted po) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/" + po + " ...");
        if (po.isSuccess())
            ctx.logSwiftlet.logInformation(ctx.storeSwiftlet.getName(), toString() + "/Backup completed, save set: " + currentSaveSet);
        else
            ctx.logSwiftlet.logError(ctx.storeSwiftlet.getName(), toString() + "/Backup completed, save set: " + currentSaveSet + ", exception=" + po.getException());
        backupActive = false;
        if (finishedListener != null) {
            finishedListener.backupFinished(po.isSuccess(), po.getException());
            finishedListener = null;
        }
        currentSaveSet = null;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(Close po) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/" + po + " ...");
        po.getSemaphore().notifySingleWaiter();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/close ...");
        Semaphore sem = new Semaphore();
        pipelineQueue.enqueue(new Close(sem));
        sem.waitHere();
        pipelineQueue.close();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/close done");
    }

    public String toString() {
        return "BackupProcessor";
    }
}
