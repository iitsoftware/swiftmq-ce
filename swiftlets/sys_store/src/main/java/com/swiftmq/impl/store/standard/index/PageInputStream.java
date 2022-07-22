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
import com.swiftmq.tools.collection.IntRingBuffer;
import com.swiftmq.tools.util.LengthCaptureDataInput;

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;

public class PageInputStream implements LengthCaptureDataInput {
    PageRecorder pageRecorder = null;
    char[] str = new char[256];
    byte[] bytearr = new byte[256];
    StoreContext ctx;
    int rootPageNo = -1;
    IntRingBuffer pageList = new IntRingBuffer(5);
    MessagePage actPage = null;
    int pos = 0;
    int length = 0;
    long captureLength = 0;

    public PageInputStream(StoreContext ctx) {
        this.ctx = ctx;
    }

    public void startCaptureLength() {
        captureLength = 0;
    }

    public long stopCaptureLength() {
        return captureLength;
    }

    public void setPageRecorder(PageRecorder pageRecorder) {
        this.pageRecorder = pageRecorder;
    }

    public void setRootPageNo(int rootPageNo) throws Exception {
        this.rootPageNo = rootPageNo;
        actPage = null;
        pos = 0;
        flipPage(rootPageNo);
    }

    private void flipPage(int pageNo) throws IOException {
        try {
            actPage = new MessagePage(ctx.cacheManager.fetchAndPin(pageNo));
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", toString() + "/flipPage, pageNo=" + pageNo + ", data length=" + actPage.getLength() + " ...");
            pageList.add(pageNo);
            pos = MessagePage.START_DATA;
            length = actPage.getLength();
        } catch (Exception e) {
            throw new IOException(e.toString());
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/flipPage, pageNo=" + pageNo + " pageRecorder=" + pageRecorder);
        if (pageRecorder != null)
            pageRecorder.recordPageNo(pageNo);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/flipPage, pageNo=" + pageNo + " done");
    }

    public int read() throws IOException {
        if (pos > length) {
            int next = actPage.getNextPage();
            if (next == -1)
                return -1;
            flipPage(next);
        }
        captureLength++;
        int b = actPage.page.data[pos++] & 0xff;
        return b;
    }

    public void readFully(byte b[]) throws IOException {
        readFully(b, 0, b.length);
    }

    public void readFully(byte b[], int off, int len) throws IOException {
        captureLength += len;
        if (pos + len <= length) {
            System.arraycopy(actPage.page.data, pos, b, off, len);
            pos += len;
        } else {
            while (len > 0) {
                if (pos == length) {
                    int next = actPage.getNextPage();
                    if (next == -1)
                        throw new EOFException();
                    flipPage(next);
                }
                int m = Math.min(length - pos, len);
                System.arraycopy(actPage.page.data, pos, b, off, m);
                off += m;
                len -= m;
                pos += m;
            }
        }
    }

    public int skipBytes(int n) throws IOException {
        return 0;
    }

    public boolean readBoolean() throws IOException {
        int ch = read();
        if (ch < 0)
            throw new EOFException();
        return (ch != 0);
    }

    public byte readByte() throws IOException {
        int ch = read();
        if (ch < 0)
            throw new EOFException();
        return (byte) (ch);
    }

    public int readUnsignedByte() throws IOException {
        int ch = read();
        if (ch < 0)
            throw new EOFException();
        return ch;
    }

    public short readShort() throws IOException {
        int ch1 = read();
        int ch2 = read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (short) ((ch1 << 8) + (ch2 << 0));
    }

    public int readUnsignedShort() throws IOException {
        int ch1 = read();
        int ch2 = read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (ch1 << 8) + (ch2 << 0);
    }

    public char readChar() throws IOException {
        int ch1 = read();
        int ch2 = read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (char) ((ch1 << 8) + (ch2 << 0));
    }

    public int readInt() throws IOException {
        int ch1 = read();
        int ch2 = read();
        int ch3 = read();
        int ch4 = read();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    public long readLong() throws IOException {
        return ((long) (readInt()) << 32) + (readInt() & 0xFFFFFFFFL);
    }

    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    public String readLine() throws IOException {
        return null;
    }

    public String readUTF() throws IOException {
        int utflen = readUnsignedShort();
        if (str.length < utflen)
            str = new char[utflen];
        if (bytearr.length < utflen)
            bytearr = new byte[utflen];
        int c, char2, char3;
        int count = 0;
        int strlen = 0;

        readFully(bytearr, 0, utflen);

        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;
            switch (c >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    /* 0xxxxxxx*/
                    count++;
                    str[strlen++] = (char) c;
                    break;
                case 12:
                case 13:
                    /* 110x xxxx   10xx xxxx*/
                    count += 2;
                    if (count > utflen)
                        throw new UTFDataFormatException();
                    char2 = (int) bytearr[count - 1];
                    if ((char2 & 0xC0) != 0x80)
                        throw new UTFDataFormatException();
                    str[strlen++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
                    break;
                case 14:
                    /* 1110 xxxx  10xx xxxx  10xx xxxx */
                    count += 3;
                    if (count > utflen)
                        throw new UTFDataFormatException();
                    char2 = (int) bytearr[count - 2];
                    char3 = (int) bytearr[count - 1];
                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                        throw new UTFDataFormatException();
                    str[strlen++] = (char) (((c & 0x0F) << 12) |
                            ((char2 & 0x3F) << 6) |
                            ((char3 & 0x3F) << 0));
                    break;
                default:
                    /* 10xx xxxx,  1111 xxxx */
                    throw new UTFDataFormatException();
            }
        }
        // The number of chars produced may be less than utflen
        return new String(str, 0, strlen);
    }

    public void unloadPages() throws IOException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/unloadPages, pageList=" + pageList + ", actPage=" + actPage);
        try {
            while (pageList.getSize() > 0) {
                ctx.cacheManager.unpin(pageList.remove());
            }
            pageList.clear();
        } catch (Exception e) {
            throw new IOException(e.toString());
        }
    }

    public void reset() throws IOException {
        actPage = null;
    }
}

