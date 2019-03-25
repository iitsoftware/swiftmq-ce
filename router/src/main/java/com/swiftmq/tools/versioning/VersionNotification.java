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

public class VersionNotification implements VersionObject, Dumpable {
    String identifier = null;
    int[] acceptedVersions = null;

    public VersionNotification(String identifier, int[] acceptedVersions) {
        this.identifier = identifier;
        this.acceptedVersions = acceptedVersions;
    }

    public VersionNotification() {
    }

    public int getDumpId() {
        return VersionObjectFactory.VERSION_NOTIFICATION;
    }

    public void writeContent(DataOutput out)
            throws IOException {
        if (identifier != null) {
            out.writeByte(1);
            out.writeUTF(identifier);
        } else
            out.writeByte(0);
        out.writeInt(acceptedVersions.length);
        for (int i = 0; i < acceptedVersions.length; i++)
            out.writeInt(acceptedVersions[i]);
    }

    public void readContent(DataInput in)
            throws IOException {
        byte set = in.readByte();
        if (set == 1)
            identifier = in.readUTF();
        acceptedVersions = new int[in.readInt()];
        for (int i = 0; i < acceptedVersions.length; i++)
            acceptedVersions[i] = in.readInt();
    }

    public String getIdentifier() {
        return identifier;
    }

    public int[] getAcceptedVersions() {
        return acceptedVersions;
    }

    public void accept(VersionVisitor visitor) {
        visitor.visit(this);
    }

    private String print(int[] a) {
        StringBuffer b = new StringBuffer("[");
        for (int i = 0; i < a.length; i++) {
            if (i > 0)
                b.append(", ");
            b.append(a[i]);
        }
        b.append("]");
        return b.toString();
    }

    public String toString() {
        return "[VersionNotification, identifier=" + identifier + ", acceptedVersion=" + print(acceptedVersions) + "]";
    }
}
