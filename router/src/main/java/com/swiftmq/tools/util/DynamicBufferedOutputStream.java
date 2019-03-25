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

package com.swiftmq.tools.util;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class DynamicBufferedOutputStream extends FilterOutputStream {
    protected static final int DEFAULT_MAX_BUFFER_SIZE = 8192; // usual TCP Buffer Size
    protected static final int DEFAULT_EXTEND_SIZE = 512;

    int maxSize = DEFAULT_MAX_BUFFER_SIZE;
    int extendSize = DEFAULT_EXTEND_SIZE;

    protected byte buf[];

    protected int count;

    public DynamicBufferedOutputStream(OutputStream out, int extendSize, int maxSize) {
        super(out);
        if (maxSize > 0) {
            this.maxSize = maxSize;
        }
        if (extendSize > 0) {
            this.extendSize = extendSize;
        }
        buf = new byte[extendSize];
    }

    public DynamicBufferedOutputStream(OutputStream out) {
        this(out, DEFAULT_EXTEND_SIZE, DEFAULT_MAX_BUFFER_SIZE);
    }

    public DynamicBufferedOutputStream(OutputStream out, int size) {
        this(out, DEFAULT_EXTEND_SIZE, size);
    }

    /**
     * Flush the internal buffer
     */
    private void flushBuffer() throws IOException {
        if (count > 0) {
            out.write(buf, 0, count);
            count = 0;
        }
    }

    private void extendBuffer(int n) throws IOException {
        int newSize = count + n;
        byte[] b = new byte[newSize];
        System.arraycopy(buf, 0, b, 0, buf.length);
        buf = b;
    }

    /**
     * Writes the specified byte to this buffered output stream.
     *
     * @param b the byte to be written.
     * @throws IOException if an I/O error occurs.
     */
    public synchronized void write(int b) throws IOException {
        if (count >= buf.length)
            extendBuffer(extendSize);
        buf[count++] = (byte) b;
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array
     * starting at offset <code>off</code> to this buffered output stream.
     *
     * <p> Ordinarily this method stores bytes from the given array into this
     * stream's buffer, flushing the buffer to the underlying output stream as
     * needed.  If the requested length is at least as large as this stream's
     * buffer, however, then this method will flush the buffer and write the
     * bytes directly to the underlying output stream.  Thus redundant
     * <code>BufferedOutputStream</code>s will not copy data unnecessarily.
     *
     * @param b   the data.
     * @param off the start offset in the data.
     * @param len the number of bytes to write.
     * @throws IOException if an I/O error occurs.
     */
    public synchronized void write(byte b[], int off, int len) throws IOException {
        // Otherwise too slow
        //if (len > maxSize-count)
        //{
        //	flushBuffer();
        //	out.write(b,off,len);
        //	return;
        //}
        if (len > buf.length - count)
            extendBuffer(len);
        System.arraycopy(b, off, buf, count, len);
        count += len;
    }

    /**
     * Flushes this buffered output stream. This forces any buffered
     * output bytes to be written out to the underlying output stream.
     *
     * @throws IOException if an I/O error occurs.
     * @see java.io.FilterOutputStream#out
     */
    public synchronized void flush() throws IOException {
        flushBuffer();
        out.flush();
    }
}
