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

import com.swiftmq.swiftlet.store.DurableStoreEntry;
import com.swiftmq.swiftlet.store.DurableSubscriberStore;
import com.swiftmq.swiftlet.store.StoreException;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DurableSubscriberStoreImpl
        implements DurableSubscriberStore, Iterator {
    static final String DELIMITER = "$";
    static final String EXTENSION = ".durable";

    StoreContext ctx = null;
    String path;
    File dir = null;
    String[] iterFiles = null;
    int iterPos = 0;
    String iterClientId = null;
    String iterDurableName = null;

    protected DurableSubscriberStoreImpl(StoreContext ctx, String path) throws StoreException {
        this.ctx = ctx;
        this.path = path;
        dir = new File(path);
        if (!dir.exists())
            throw new StoreException(path + " doesn't exists");
        if (!dir.isDirectory())
            throw new StoreException(path + " is not a directory");
    }

    public static String getDurableFilename(String clientId, String durableName) {
        return clientId + DELIMITER + durableName + EXTENSION;
    }

    public DurableSubscriberStoreImpl newInstance() throws StoreException {
        return new DurableSubscriberStoreImpl(ctx, path);
    }

    public synchronized Iterator iterator()
            throws StoreException {
        iterFiles = dir.list(new FilenameFilter() {
            public boolean accept(File d, String name) {
                return name.endsWith(EXTENSION);
            }
        });
        iterPos = 0;
        return this;
    }

    public synchronized boolean hasNext() {
        return iterFiles != null && iterPos < iterFiles.length;
    }

    public synchronized Object next() {
        String name = iterFiles[iterPos];
        iterClientId = name.substring(0, name.indexOf(DELIMITER));
        iterDurableName = name.substring(name.indexOf(DELIMITER) + 1, name.indexOf(EXTENSION));
        DurableStoreEntry entry = null;
        try {
            entry = getDurableStoreEntry(iterClientId, iterDurableName);
        } catch (Exception e) {
            throw new RuntimeException(e.toString());
        }
        iterPos++;
        return entry;
    }

    public synchronized void remove() {
        try {
            deleteDurableStoreEntry(iterClientId, iterDurableName);
        } catch (Exception e) {
            throw new RuntimeException(e.toString());
        }
    }

    public synchronized DurableStoreEntry getDurableStoreEntry(String clientId, String durableName)
            throws StoreException {
        String filename = getDurableFilename(clientId, durableName);
        File f = new File(dir, filename);
        if (!f.exists())
            throw new StoreException(f.getAbsolutePath() + " does not exists");
        Map map = null;
        try {
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
            map = (HashMap) ois.readObject();
            ois.close();
        } catch (Exception e) {
            throw new StoreException(e.toString());
        }
        DurableStoreEntry entry = new DurableStoreEntry(clientId,
                durableName,
                (String) map.get("topicName"),
                (String) map.get("selector"),
                map.get("noLocal") != null);

        return entry;
    }

    public synchronized void insertDurableStoreEntry(DurableStoreEntry durableStoreEntry)
            throws StoreException {
        Map map = new HashMap();
        map.put("topicName", durableStoreEntry.getTopicName());
        if (durableStoreEntry.getSelector() != null)
            map.put("selector", durableStoreEntry.getSelector());
        if (durableStoreEntry.isNoLocal())
            map.put("noLocal", "true");
        try {
            String filename = getDurableFilename(durableStoreEntry.getClientId(), durableStoreEntry.getDurableName());
            File f = new File(dir, filename);
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(f));
            oos.writeObject(map);
            oos.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new StoreException(e.toString());
        }
    }

    public synchronized void deleteDurableStoreEntry(String clientId, String durableName)
            throws StoreException {
        String filename = getDurableFilename(clientId, durableName);
        File f = new File(dir, filename);
        if (f.exists())
            f.delete();
    }

    private void copyFile(File source, File dest) throws Exception {
        byte[] b = new byte[8192];
        int n = 0;
        FileInputStream in = new FileInputStream(source);
        FileOutputStream out = new FileOutputStream(dest);
        while ((n = in.read(b)) != -1)
            out.write(b, 0, n);
        out.getFD().sync();
        out.close();
        in.close();
    }

    public synchronized void copy(String newPath) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/copy ...");
        String[] files = dir.list(new FilenameFilter() {
            public boolean accept(File d, String name) {
                return name.endsWith(EXTENSION);
            }
        });
        if (files != null) {
            for (int i = 0; i < files.length; i++) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/copy, file=" + files[i]);
                File source = new File(dir, files[i]);
                File dest = new File(newPath + File.separatorChar + files[i]);
                copyFile(source, dest);
            }
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/copy done");
    }

    public void close()
            throws StoreException {
        // do nothing
    }

    public String toString() {
        return "[DurableSubscriberStoreImpl, path=" + path + "]";
    }
}

