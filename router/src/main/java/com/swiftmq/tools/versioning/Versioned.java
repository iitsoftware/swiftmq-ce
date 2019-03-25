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

package com.swiftmq.tools.versioning;

import com.swiftmq.tools.dump.Dumpable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Versioned implements VersionObject, Dumpable {
    int version = 0;
    byte[] payload = null;
    int length = 0;

    public Versioned(int version, byte[] newpayload, int length) {
        this.version = version;
        this.length = length;
        payload = new byte[length];
        System.arraycopy(newpayload, 0, payload, 0, length);
    }

    public Versioned() {
    }

    public int getDumpId() {
        return VersionObjectFactory.VERSIONED;
    }

    public void writeContent(DataOutput out)
            throws IOException {
        out.writeInt(version);
        if (payload != null) {
            out.writeByte(1);
            out.writeInt(length);
            out.write(payload, 0, length);
        } else
            out.writeByte(0);
    }

    public void readContent(DataInput in)
            throws IOException {
        version = in.readInt();
        byte set = in.readByte();
        if (set == 1) {
            length = in.readInt();
            payload = new byte[length];
            in.readFully(payload);
        }
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public void accept(VersionVisitor visitor) {
        visitor.visit(this);
    }

    public String toString() {
        return "[Versioned, version=" + version + ", length=" + length + ", payload=" + payload + "]";
    }
}
