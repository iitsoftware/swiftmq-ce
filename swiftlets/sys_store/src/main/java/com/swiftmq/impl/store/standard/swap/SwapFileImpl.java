
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

package com.swiftmq.impl.store.standard.swap;

import com.swiftmq.jms.MessageImpl;
import com.swiftmq.swiftlet.store.StoreEntry;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.File;
import java.io.RandomAccessFile;

public class SwapFileImpl implements SwapFile {
    static final int BUFFER_SIZE = 2048;
    protected String path = null;
    protected String filename = null;
    long maxLength = 0;
    File f = null;
    RandomAccessFile file = null;
    DataByteArrayInputStream dis = null;
    DataByteArrayOutputStream dos = null;
    byte[] buffer = null;
    protected long numberMessages = 0;

    public SwapFileImpl(String path, String filename, long maxLength)
            throws Exception {
        this.filename = filename;
        this.maxLength = maxLength;
        f = new File(path + File.separatorChar + filename);
        file = new RandomAccessFile(f, "rw");
        buffer = new byte[BUFFER_SIZE];
        dis = new DataByteArrayInputStream(buffer);
        dos = new DataByteArrayOutputStream(2048);
    }

    public String getPath() {
        return path;
    }

    public String getFilename() {
        return filename;
    }

    public long getNumberMessages() {
        return numberMessages;
    }

    public void setNumberMessages(long numberMessages) {
        this.numberMessages = numberMessages;
    }

    public long getMaxLength() {
        return maxLength;
    }

    public boolean hasSpace() throws Exception {
        return file.length() < maxLength;
    }

    public StoreEntry get(long fp)
            throws Exception {
        file.seek(fp);
        int len = file.readInt();
        if (buffer.length - 1 < len) {
            buffer = new byte[len];
            dis.setBuffer(buffer);
        }
        file.read(buffer, 0, len);
        StoreEntry storeEntry = new StoreEntry();
        storeEntry.key = null;
        storeEntry.deliveryCount = dis.readInt();
        storeEntry.message = MessageImpl.createInstance(dis.readInt());
        storeEntry.message.readContent(dis);
        dis.reset();
        return storeEntry;
    }

    public long add(StoreEntry storeEntry)
            throws Exception {
        numberMessages++;
        file.seek(file.length());
        long address = file.getFilePointer();
        dos.writeInt(storeEntry.deliveryCount);
        storeEntry.message.writeContent(dos);
        file.writeInt(dos.getCount());
        file.write(dos.getBuffer(), 0, dos.getCount());
        dos.rewind();
        return address;
    }

    public void updateDeliveryCount(long fp, int deliveryCount)
            throws Exception {
        file.seek(fp + 4);
        file.writeInt(deliveryCount);
    }

    public void remove(long fp)
            throws Exception {
        numberMessages--;
        // do nothing
    }

    public void closeNoDelete() {
        try {
            file.close();
        } catch (Exception ignored) {
        }
    }

    public void close() {
        closeNoDelete();
        f.delete();
    }

    public String toString() {
        return "[SwapFile, filename=" + filename + ", nMsgs=" + numberMessages + "]";
    }
}

