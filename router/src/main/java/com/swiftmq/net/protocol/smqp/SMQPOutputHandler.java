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

package com.swiftmq.net.protocol.smqp;

import com.swiftmq.net.protocol.ProtocolOutputHandler;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A SMQPOutputHandler handles SMQP output.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class SMQPOutputHandler extends ProtocolOutputHandler {
    final static int BUFFER_SIZE = 1024 * 128;
    final static int EXTEND_SIZE = 1024 * 64;
    static final boolean DEBUG = Boolean.valueOf(System.getProperty("swiftmq.smqp.handler.debug", "false")).booleanValue();
    static final boolean USE_THREAD_LOCAL = Boolean.valueOf(System.getProperty("swiftmq.smqp.handler.buffer.threadlocal", "true")).booleanValue();
    static final SimpleDateFormat format = new SimpleDateFormat("dd.MM.yyyy-HH:mm:ss.SSS");

    int bufferSize = BUFFER_SIZE;
    int extendSize = EXTEND_SIZE;
    byte[] currentInput = null;
    int inputLength = 0;
    byte[] currentOutput = null;
    int outputOffset = 0;
    int outputLength = 0;
    int chunkCount = 0;
    long gclen = 0;
    static ThreadLocal bufferHolder = USE_THREAD_LOCAL ? new ThreadLocal() : null;

    public SMQPOutputHandler(int bufferSize, int extendSize) {
        this.bufferSize = bufferSize;
        this.extendSize = extendSize;
    }

    public SMQPOutputHandler() {
        this(BUFFER_SIZE, EXTEND_SIZE);
    }

    private int readLength(byte[] b) {
        int i1 = b[0] & 0xff;
        int i2 = b[1] & 0xff;
        int i3 = b[2] & 0xff;
        int i4 = b[3] & 0xff;
        int i = (i1 << 24) + (i2 << 16) + (i3 << 8) + (i4 << 0);
        return i;
    }

    private void writeLength(byte[] b, int length) {
        b[0] = (byte) ((length >>> 24) & 0xFF);
        b[1] = (byte) ((length >>> 16) & 0xFF);
        b[2] = (byte) ((length >>> 8) & 0xFF);
        b[3] = (byte) ((length >>> 0) & 0xFF);
    }

    private void ensureInput(int size) {
        if (currentInput == null) {
            byte[] b = bufferHolder != null ? (byte[]) bufferHolder.get() : null;
            if (b == null) {
                currentInput = new byte[Math.max(bufferSize, size)];
                if (DEBUG)
                    System.out.println(format.format(new Date()) + "/" + super.toString() + ", create buffer, len=" + currentInput.length);
            } else {
                if (DEBUG)
                    System.out.println(format.format(new Date()) + "/" + super.toString() + ", got buffer " + b + ", len=" + b.length + " from bufferHolder of thread: " + Thread.currentThread());
                currentInput = b;
            }
            inputLength = 4;
        } else {
            if (currentInput.length - inputLength < size) {
                gclen += currentInput.length;
                byte b[] = new byte[Math.max(currentInput.length + extendSize, size + inputLength)];
                System.arraycopy(currentInput, 0, b, 0, inputLength);
                currentInput = b;
                if (DEBUG)
                    System.out.println(format.format(new Date()) + "/" + super.toString() + ", extend buffer, gced=" + gclen + ", len=" + currentInput.length);
            }
        }
    }

    public ProtocolOutputHandler create() {
        return new SMQPOutputHandler();
    }

    public ProtocolOutputHandler create(int bufferSize, int extendSize) {
        return new SMQPOutputHandler(bufferSize, extendSize);
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
            inputLength = 4;
            chunkCount = 0;
            if (bufferHolder != null) {
                if (DEBUG)
                    System.out.println(format.format(new Date()) + "/" + super.toString() + ", buffer " + currentInput + ", len=" + currentInput.length + " stored in bufferHolder of thread: " + Thread.currentThread());
                bufferHolder.set(currentInput);
                currentInput = null;
            }
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
        writeLength(currentInput, inputLength - 4);
        chunkCount = 1;
    }

    public String toString() {
        return "[SMQPOutputHandler]";
    }
}

