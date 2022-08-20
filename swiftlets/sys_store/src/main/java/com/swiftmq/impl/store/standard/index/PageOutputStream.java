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

package com.swiftmq.impl.store.standard.index;

import com.swiftmq.impl.store.standard.StoreContext;
import com.swiftmq.impl.store.standard.cache.Page;
import com.swiftmq.impl.store.standard.log.InsertLogAction;
import com.swiftmq.impl.store.standard.log.UpdatePortionLogAction;
import com.swiftmq.impl.store.standard.pagedb.PageSize;
import com.swiftmq.tools.collection.RingBuffer;

import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.List;

public class PageOutputStream implements DataOutput {
    StoreContext ctx;
    int rootPageNo = -1;
    List journal = null;
    RingBuffer pages = new RingBuffer(5);
    MessagePage actPage = null;
    int available = 0;
    int pos = 0;

    PageOutputStream(StoreContext ctx) {
        this.ctx = ctx;
    }

    void setJournal(List journal) {
        this.journal = journal;
    }

    int getRootPageNo() {
        return rootPageNo;
    }

    private byte[] getPortion() {
        actPage.setLength(pos);
        byte b[] = new byte[pos];
        System.arraycopy(actPage.page.data, 0, b, 0, b.length);
        return b;
    }

    private void flipPage() throws IOException {
        try {
            Page page = ctx.cacheManager.createAndPin();
            page.dirty = true;
            page.empty = false;
            if (rootPageNo == -1)
                rootPageNo = page.pageNo;
            pages.add(page);
            MessagePage mp = new MessagePage(page);
            if (actPage != null) {
                actPage.setNextPage(page.pageNo);
                mp.setPrevPage(actPage.page.pageNo);
                journal.add(new UpdatePortionLogAction(actPage.page.pageNo, 0, null, getPortion()));
            } else
                mp.setPrevPage(-1);
            mp.setNextPage(-1);
            actPage = mp;
            available = PageSize.getCurrent() - MessagePage.START_DATA;
            pos = MessagePage.START_DATA;
            journal.add(new InsertLogAction(page.pageNo));
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e.toString());
        }
    }

    public void write(int b) throws IOException {
        if (available == 0)
            flipPage();
        actPage.page.data[pos++] = (byte) b;
        available--;
    }

    public void write(byte b[], int off, int len) throws IOException {
        int n = 0;
        while (n < len) {
            if (available == 0)
                flipPage();
            int t = available < len - n ? available : len - n;
            System.arraycopy(b, off + n, actPage.page.data, pos, t);
            n += t;
            available -= t;
            pos += t;
        }
    }

    public void write(byte b[]) throws IOException {
        write(b, 0, b.length);
    }

    public void writeBoolean(boolean v) throws IOException {
        write(v ? 1 : 0);
    }

    public void writeByte(int v) throws IOException {
        write(v);
    }

    public void writeShort(int v) throws IOException {
        write((v >>> 8) & 0xFF);
        write((v >>> 0) & 0xFF);
    }

    public void writeChar(int v) throws IOException {
        write((v >>> 8) & 0xFF);
        write((v >>> 0) & 0xFF);
    }

    public void writeInt(int v) throws IOException {
        write((v >>> 24) & 0xFF);
        write((v >>> 16) & 0xFF);
        write((v >>> 8) & 0xFF);
        write((v >>> 0) & 0xFF);
    }

    public void writeLong(long v) throws IOException {
        write((int) (v >>> 56) & 0xFF);
        write((int) (v >>> 48) & 0xFF);
        write((int) (v >>> 40) & 0xFF);
        write((int) (v >>> 32) & 0xFF);
        write((int) (v >>> 24) & 0xFF);
        write((int) (v >>> 16) & 0xFF);
        write((int) (v >>> 8) & 0xFF);
        write((int) (v >>> 0) & 0xFF);
    }

    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    public void writeBytes(String s) throws IOException {
        int len = s.length();
        for (int i = 0; i < len; i++)
            write((byte) s.charAt(i));
    }

    public void writeChars(String s) throws IOException {
        int len = s.length();
        for (int i = 0; i < len; i++) {
            int v = s.charAt(i);
            write((v >>> 8) & 0xFF);
            write((v >>> 0) & 0xFF);
        }
    }

    public void writeUTF(String str) throws IOException {
        int strlen = str.length();
        int utflen = 0;
        int c = 0;

        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                utflen++;
            } else if (c > 0x07FF) {
                utflen += 3;
            } else {
                utflen += 2;
            }
        }

        if (utflen > 65535)
            throw new UTFDataFormatException();
        writeShort(utflen);
        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                write((byte) c);
            } else if (c > 0x07FF) {
                write((byte) (0xE0 | ((c >> 12) & 0x0F)));
                write((byte) (0x80 | ((c >> 6) & 0x3F)));
                write((byte) (0x80 | ((c >> 0) & 0x3F)));
            } else {
                write((byte) (0xC0 | ((c >> 6) & 0x1F)));
                write((byte) (0x80 | ((c >> 0) & 0x3F)));
            }
        }
    }

    public void flush() throws IOException {
        journal.add(new UpdatePortionLogAction(actPage.page.pageNo, 0, null, getPortion()));
    }

    void unloadPages() throws Exception {
        while (pages.getSize() > 0)
            ctx.cacheManager.unpin(((Page) pages.remove()).pageNo);
        pages.clear();
    }

    void reset() {
        rootPageNo = -1;
        actPage = null;
        available = 0;
        pos = 0;
    }
}

