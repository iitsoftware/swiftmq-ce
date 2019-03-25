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

import java.nio.ByteBuffer;

/**
 * A ProtocolInputHandler receives bytes in form of byte arrays from the Network Swiftlet.
 * Implementing classes handle everything protocol specific and detect whether a so-called
 * chunk is completed. That is, for example, if there is a protocol which receives Strings,
 * where each String is terminated by a '\n', thus a line reader, a chunk is completed when
 * a '\n' is detected. By detecting a completed chunk, the ProtocolInputHandler calls the
 * registered ChunkListener which in turn should process the chunk. <br><br>
 * This procedure is required to handle blocking and nonblocking I/O transparently.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public interface ProtocolInputHandler {

    /**
     * Factory method to create a new ProtocolInputHandler.
     * For example, a RawInputHandler returns a RawInputHandler here.
     *
     * @return new protocol input handler.
     */
    public ProtocolInputHandler create();

    /**
     * Create the input buffer of the protocol handler.
     * The buffer must be created with the <code>initialSize</code>. Each time
     * <code>getBuffer()</code> is called, the buffer must have a remaining size
     * of <code>ensureSize</code>.
     *
     * @param initialSize initial size
     * @param ensureSize  ensured size on each getBuffer call
     */
    public void createInputBuffer(int initialSize, int ensureSize);

    /**
     * Returns the protocol handler input buffer.
     *
     * @return buffer.
     */
    public ByteBuffer getByteBuffer();

    /**
     * Returns the protocol handler input buffer.
     *
     * @return buffer.
     */
    public byte[] getBuffer();

    /**
     * Returns the current offset of the input buffer.
     *
     * @return offset.
     */
    public int getOffset();

    /**
     * Set the number of bytes written into the buffer.
     * Called from the Network Swiftlet after it has read bytes from sockets directly into the buffer+offset.
     *
     * @param written number of bytes written
     */
    public void setBytesWritten(int written);

    /**
     * Set the ChunkListener.
     * The ChunkListener is implemented by the Network Swiftlet and will be set from it.
     *
     * @param listener listener.
     */
    public void setChunkListener(ChunkListener listener);

}

