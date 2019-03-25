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

package com.swiftmq.amqp;

import com.swiftmq.tools.concurrent.AsyncCompletionCallback;
import com.swiftmq.tools.concurrent.Semaphore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Protocol header for AMQP and SASL version negotiation.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class ProtocolHeader implements Writable {
    Charset charset = Charset.forName("US-ASCII");
    byte[] b = new byte[8];

    /**
     * Constructs a ProtocolHeader
     *
     * @param name     Must be "AMQP"
     * @param id       Protocol Id
     * @param major    Protocol major version
     * @param minor    Protocol minor version
     * @param revision Protocol revision
     */
    public ProtocolHeader(String name, int id, int major, int minor, int revision) {
        byte[] bytes = charset.encode(name).array();
        System.arraycopy(bytes, 0, b, 0, 4);
        b[4] = (byte) id;
        b[5] = (byte) major;
        b[6] = (byte) minor;
        b[7] = (byte) revision;
    }

    /**
     * Constructs an empty ProtocolHeader to read it from a stream by readContent().
     */
    public ProtocolHeader() {
    }

    /**
     * Returns the protocol name.
     *
     * @return protocol name
     */
    public String getName() {
        return charset.decode(ByteBuffer.wrap(b, 0, 4)).toString();
    }

    /**
     * Returns the protocol id.
     *
     * @return protocol id
     */
    public int getId() {
        return b[4];
    }

    /**
     * Return the major version.
     *
     * @return major version
     */
    public int getMajor() {
        return b[5];
    }

    /**
     * Returns the minor version.
     *
     * @return minor version
     */
    public int getMinor() {
        return b[6];
    }

    /**
     * Returns the revision.
     *
     * @return revision
     */
    public int getRevision() {
        return b[7];
    }

    public Semaphore getSemaphore() {
        return null;
    }

    public AsyncCompletionCallback getCallback() {
        return null;
    }

    public void readContent(DataInput in) throws IOException {
        in.readFully(b);
    }

    public void writeContent(DataOutput out) throws IOException {
        out.write(b);
    }

    public boolean equals(Object o) {
        ProtocolHeader that = (ProtocolHeader) o;
        return getName().equals(that.getName()) && getId() == that.getId() && getMajor() == that.getMajor() && getMinor() == that.getMinor() && getRevision() == that.getRevision();
    }

    public String toString() {
        return "[ProtocolHeader, name=" + getName() + ", id=" + getId() + ", major=" + getMajor() + ", minor=" + getMinor() + ", revision=" + getRevision() + "]";
    }
}
