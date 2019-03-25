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

import java.io.IOException;
import java.io.InputStream;

public class MultiByteArrayInputStream extends InputStream {

    byte[][] buffers;

    int pos;
    int totalSize;

    byte[] currentBuffer;
    int currentBufferIndex;
    int currentBufferPosition;

    public MultiByteArrayInputStream(byte[][] buffers, int totalSize) {
        this.buffers = buffers;
        this.pos = 0;
        this.totalSize = totalSize;
        this.currentBuffer = buffers[0];
        this.currentBufferIndex = 0;
        this.currentBufferPosition = 0;
    }

    public int read() {
        pos++;
        if (currentBufferPosition < currentBuffer.length)
            return currentBuffer[currentBufferPosition++] & 0xff;
        if (currentBufferIndex < buffers.length - 1) {
            currentBuffer = buffers[++currentBufferIndex];
            currentBufferPosition = 0;
            return currentBuffer[currentBufferPosition++] & 0xff;
        }
        return -1;
    }

    public int read(byte b[], int off, int len) {
        if (pos >= totalSize)
            return -1;
        if (pos + len > totalSize)
            len = totalSize - pos;
        if (len <= 0)
            return 0;
        int toCopy = len;
        while (toCopy > 0) {
            if (currentBufferPosition == currentBuffer.length) {
                currentBuffer = buffers[++currentBufferIndex];
                currentBufferPosition = 0;
            }
            int copyFromThisBuffer = Math.min(toCopy, currentBuffer.length - currentBufferPosition);
            System.arraycopy(currentBuffer, currentBufferPosition, b, off, copyFromThisBuffer);
            currentBufferPosition += copyFromThisBuffer;
            off += copyFromThisBuffer;
            toCopy -= copyFromThisBuffer;
        }

        pos += len;
        return len;
    }

    public long skip(long n) {
        throw new UnsupportedOperationException();
    }

    public int available() {
        return totalSize - pos;
    }

    public boolean markSupported() {
        return false;
    }

    public void mark(int readAheadLimit) {
        throw new UnsupportedOperationException();
    }

    public void reset() {
        pos = 0;
        currentBuffer = buffers[0];
        currentBufferIndex = 0;
        currentBufferPosition = 0;
    }

    public void close() throws IOException {
        buffers = null;
        currentBuffer = null;
    }

}