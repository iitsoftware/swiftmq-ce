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

package com.swiftmq.net.protocol;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A ProtocolOutputHandler is the complement to a ProtocolInputHandler and responsible
 * for protocol specific packaging before data is sent to the network. The ProtocolOutputHandler
 * is an OutputStream and will be used as such from the Swiftlets. These just write to the
 * OutputStream and call <code>flush()</code> at the end which leads to a call to <code>markChunkCompleted()</code>
 * and then to an invocation of an registered OutputListener which performs the write to the network.
 * This could be done either blocking or non-blocking. The OutputListener returns the number of
 * bytes written to the network and the invocation continues until all bytes have been written.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public abstract class ProtocolOutputHandler extends OutputStream {
    OutputListener listener = null;


    /**
     * Set the OutputListener.
     * The OutputListener is implemented by the Network Swiftlet.
     *
     * @param listener listenr.
     */
    public final void setOutputListener(OutputListener listener) {
        this.listener = listener;
    }


    /**
     * Invokes the OutputListener.
     * Done by the Netwok Swiftlet.
     *
     * @throws IOException on error.
     */
    public final void invokeOutputListener()
            throws IOException {
        while (getChunkCount() > 0)
            setBytesWritten(listener.performWrite(getByteArray(), getOffset(), getLength()));
    }

    public void write(int b) throws IOException {
        addByte((byte) b);
    }

    public void write(byte[] b, int offset, int len) throws IOException {
        addBytes(b, offset, len);
    }

    public void flush() throws IOException {
        markChunkCompleted();
    }

    /**
     * Factory method to create a new ProtocolOutputHandler.
     * For example, a RawOutputHandler returns a RawOutputHandler here.
     *
     * @return new protocol input handler.
     */
    public abstract ProtocolOutputHandler create();

    /**
     * Factory method to create a new ProtocolOutputHandler.
     * For example, a RawOutputHandler returns a RawOutputHandler here.
     *
     * @param bufferSize initial buffer size
     * @param extendSize extend size
     * @return new protocol input handler.
     */
    public abstract ProtocolOutputHandler create(int bufferSize, int extendSize);


    /**
     * Returns the number of chunk the handler has stored.
     *
     * @return number of chunks.
     */
    public abstract int getChunkCount();


    /**
     * Marks a chunk completed. Called during <code>flush()</code>
     * If a protocol transmits byte streams with a length field in front, this is the place
     * to determine the length.
     */
    protected abstract void markChunkCompleted();


    /**
     * Returns a reference to the byte array of the current chunk to transmit.
     * Called during <code>invokeOutputListener()</code>.
     *
     * @return byte array.
     */
    protected abstract byte[] getByteArray();


    /**
     * Returns the offset in the byte array of the current chunk where transmit should start.
     * Called during <code>invokeOutputListener()</code>.
     *
     * @return offset.
     */
    protected abstract int getOffset();


    /**
     * Returns the number of bytes to transmit, starting at the offset.
     * Called during <code>invokeOutputListener()</code>.
     *
     * @return length.
     */
    protected abstract int getLength();


    /**
     * Sets the number of bytes written from the OutputListener.
     * This is the actual number of bytes written from the OutputListener to the network.
     * The offset then must be set to <code>offset+written</code> and the length to <code>length-written</code>.
     * If all bytes of the current chunk have been written, the chunk can be destroyed or marked for reuse.
     *
     * @param written number of written bytes.
     */
    protected abstract void setBytesWritten(int written);


    /**
     * Add a byte to the current chunk.
     * Called from <code>write(b)</code>.
     *
     * @param b byte.
     */
    protected abstract void addByte(byte b);


    /**
     * Adds a number of bytes to the current chunk.
     * Called from <code>write(b[],offset,len)</code>.
     *
     * @param b      byte array.
     * @param offset offset.
     * @param len    length.
     */
    protected abstract void addBytes(byte[] b, int offset, int len);

}

