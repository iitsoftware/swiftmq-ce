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

package com.swiftmq.impl.streams;

import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.tools.security.Store;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.RandomAccessFile;
import java.util.Arrays;

public class StreamLibDeployer extends MessageProcessor {
    static final String TP_LISTENER = "sys$streams.stream.processor";
    private static final String STREAMLIB_QUEUE = "streamlib";
    SwiftletContext ctx = null;
    ThreadPool myTP = null;
    QueueReceiver receiver = null;
    QueuePullTransaction pullTransaction = null;
    boolean closed = false;
    MessageEntry entry = null;

    public StreamLibDeployer(SwiftletContext ctx) throws Exception {
        this.ctx = ctx;
        if (!ctx.queueManager.isQueueDefined(STREAMLIB_QUEUE))
            ctx.queueManager.createQueue(STREAMLIB_QUEUE, (ActiveLogin) null);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/creating ...");
        myTP = ctx.threadpoolSwiftlet.getPool(TP_LISTENER);
        receiver = ctx.queueManager.createQueueReceiver(STREAMLIB_QUEUE, null, null);
        pullTransaction = receiver.createTransaction(false);
        pullTransaction.registerMessageProcessor(this);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/creating done");
    }

    public boolean isValid() {
        return !closed;
    }

    public void processMessage(MessageEntry entry) {
        this.entry = entry;
        myTP.dispatchTask(this);
    }

    public void processException(Exception e) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/processException: " + e);
    }

    public String getDispatchToken() {
        return TP_LISTENER;
    }

    public String getDescription() {
        return ctx.streamsSwiftlet.getName() + "/" + toString();
    }

    public static byte[] loadAsBytes(File file) throws Exception {
        if (!file.exists())
            throw new Exception("File not found: " + file.getName());
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        byte[] b = new byte[(int) file.length()];
        randomAccessFile.readFully(b);
        return b;
    }

    private void addCerts(String fqn, File dir) throws Exception {
        File[] certs = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".pem");
            }
        });
        if (certs != null && certs.length > 0) {
            Store store = new Store();
            for (int i = 0; i < certs.length; i++) {
                File cert = certs[i];
                ctx.logSwiftlet.logInformation(ctx.streamsSwiftlet.getName(), toString() + "/addCert: " + fqn + "." + cert.getName());
                store.addCert(fqn + "." + cert.getName(), loadAsBytes(cert));
            }
            store.save();
        }
    }

    private void removeCerts(String fqn, File dir) throws Exception {
        File[] certs = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".pem");
            }
        });
        if (certs != null && certs.length > 0) {
            Store store = new Store();
            for (int i = 0; i < certs.length; i++) {
                File cert = certs[i];
                ctx.logSwiftlet.logInformation(ctx.streamsSwiftlet.getName(), toString() + "/removeCert: " + fqn + "." + cert.getName());
                store.removeCert(fqn + "." + cert.getName());
            }
            store.save();
        }
    }

    public void removeStreamLibs(String fqn) {
        File folder = new File(ctx.streamLibDir + File.separatorChar + fqn);
        if (folder.exists() && folder.isDirectory()) {
            try {
                removeCerts(fqn, folder);
            } catch (Exception e) {
                ctx.logSwiftlet.logError(ctx.streamsSwiftlet.getName(), toString() + "/error removeCerts: " + e);
            }
            Arrays.stream(folder.listFiles()).forEach(File::delete);
            folder.delete();
        }
    }

    private File getOrCreateDeployDir(String domain, String pkg, String stream) {
        File dir = new File(ctx.streamLibDir + File.separatorChar + domain + "." + pkg + "." + stream);
        if (!dir.exists())
            dir.mkdir();
        return dir;
    }

    private void appendChunk(File dir, String libname, int chunk, byte[] buffer) throws Exception {
        File file = new File(dir, libname);
        if (file.exists()) {
            if (chunk == 0) {
                file.delete();
                file.createNewFile();
            }
        } else {
            if (chunk > 0)
                throw new Exception("Protocol error: received a chunk > 0 for a file that doesn't exists");
            file.createNewFile();
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/appendChunk, libname=" + libname + ", chunk=" + chunk);
        FileOutputStream fos = new FileOutputStream(file, true);
        fos.write(buffer);
        fos.flush();
        fos.close();
    }

    private void sendReply(QueueImpl replyTo) throws JMSException {
        QueueSender sender = ctx.queueManager.createQueueSender(replyTo.getQueueName(), (ActiveLogin) null);
        QueuePushTransaction pushTx = sender.createTransaction();
        MessageImpl reply = new MessageImpl();
        reply.setBooleanProperty("success", true);
        reply.setJMSDestination(replyTo);
        pushTx.putMessage(reply);
        pushTx.commit();
        sender.close();
    }

    private void processChunk(BytesMessage msg) throws Exception {
        String domain = msg.getStringProperty("domain");
        String pkg = msg.getStringProperty("package");
        String stream = msg.getStringProperty("stream");
        String libname = msg.getStringProperty("libname");
        int chunk = msg.getIntProperty("nchunk");
        boolean last = msg.getBooleanProperty("last");
        QueueImpl replyTo = (QueueImpl) msg.getJMSReplyTo();
        int len = (int) msg.getBodyLength();
        byte[] buffer = new byte[len];
        msg.readBytes(buffer);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/processChunk, domain=" + domain + ", package=" + pkg + ", stream=" + stream + ", libname=" + libname + ", chunk=" + chunk + ", last=" + last);
        File dir = getOrCreateDeployDir(domain, pkg, stream);
        appendChunk(dir, libname, chunk, buffer);
        if (last) {
            addCerts(domain + "." + pkg + "." + stream, dir);
            sendReply(replyTo);
        }
    }

    public void run() {
        try {
            pullTransaction.commit();
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/run, exception committing tx: " + e + ", exiting");
            return;
        }
        try {
            BytesMessageImpl msg = (BytesMessageImpl) entry.getMessage();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/run, new message: " + msg);
            processChunk(msg);
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/run, exception during processing: " + e);
            ctx.logSwiftlet.logError(ctx.streamsSwiftlet.getName(), toString() + "/run, exception during processing: " + e);
        }
        if (closed)
            return;
        try {
            pullTransaction = receiver.createTransaction(false);
            pullTransaction.registerMessageProcessor(this);
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/run, exception creating new tx: " + e + ", exiting");
            return;
        }
    }

    public void stop() {
    }

    public void close() {
        closed = true;
        try {
            receiver.close();
        } catch (Exception ignored) {
        }
    }

    @Override
    public String toString() {
        return "[StreamLibDeployer, " +
                "streamLibDir='" + ctx.streamLibDir + '\'' +
                ']';
    }
}
