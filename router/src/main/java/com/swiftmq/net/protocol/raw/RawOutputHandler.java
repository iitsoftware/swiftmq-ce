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

package com.swiftmq.net.protocol.raw;

import com.swiftmq.net.protocol.ProtocolOutputHandler;

/**
 * A RawOutputHandler handles a raw byte stream and pass them to an output listener
 * on every call to <code>flush()</code> without any protocol specifiy actions.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class RawOutputHandler extends ProtocolOutputHandler {
    final static int BUFFER_SIZE = 1024 * 128;
    final static int EXTEND_SIZE = 1024 * 64;

    int bufferSize = BUFFER_SIZE;
    int extendSize = EXTEND_SIZE;
    byte[] currentInput = null;
    int inputLength = 0;
    byte[] currentOutput = null;
    int outputOffset = 0;
    int outputLength = 0;
    int chunkCount = 0;

    public RawOutputHandler(int bufferSize, int extendSize) {
        this.bufferSize = bufferSize;
        this.extendSize = extendSize;
    }

    public RawOutputHandler() {
        this(BUFFER_SIZE, EXTEND_SIZE);
    }

    private void ensureInput(int size) {
        if (currentInput == null) {
            currentInput = new byte[Math.max(bufferSize, size)];
            inputLength = 0;
        } else {
            if (currentInput.length - inputLength < size) {
                byte b[] = new byte[Math.max(extendSize, size + inputLength)];
                System.arraycopy(currentInput, 0, b, 0, inputLength);
                currentInput = b;
            }
        }
    }

    public ProtocolOutputHandler create() {
        return new RawOutputHandler();
    }

    public ProtocolOutputHandler create(int bufferSize, int extendSize) {
        return new RawOutputHandler(bufferSize, extendSize);
    }

    public int getChunkCount() {
        return chunkCount;
    }

    protected byte[] getByteArray() {
        if (currentOutput == null) {
            currentOutput = currentInput;
            outputOffset = 0;
            outputLength = inputLength;
        }
        return currentOutput;
    }

    protected int getOffset() {
        return outputOffset;
    }

    protected int getLength() {
        return outputLength - outputOffset;
    }

    protected void setBytesWritten(int written) {
        outputOffset += written;
        if (outputOffset == outputLength) {
            currentOutput = null;
            outputOffset = 0;
            outputLength = 0;
            inputLength = 0;
            chunkCount = 0;
        }
    }

    protected void addByte(byte b) {
        ensureInput(1);
        currentInput[inputLength++] = b;
    }

    protected void addBytes(byte[] b, int offset, int len) {
        ensureInput(len);
        System.arraycopy(b, offset, currentInput, inputLength, len);
        inputLength += len;
    }

    protected void markChunkCompleted() {
        chunkCount = 1;
    }

    public String toString() {
        return "[RawOutputHandler]";
    }
}
