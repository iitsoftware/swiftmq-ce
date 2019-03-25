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

package com.swiftmq.jms;

import javax.transaction.xa.Xid;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class XidImpl implements Xid, Serializable {
    byte[] branchQualifier = null;
    int formatId = 0;
    byte[] globalTransactionId = null;
    int hash = 0;
    boolean routing = false;

    public XidImpl(byte[] branchQualifier, int formatId, byte[] globalTransactionId) {
        this.branchQualifier = branchQualifier;
        this.formatId = formatId;
        this.globalTransactionId = globalTransactionId;
    }

    public XidImpl(Xid xid) {
        this.branchQualifier = xid.getBranchQualifier();
        this.formatId = xid.getFormatId();
        this.globalTransactionId = xid.getGlobalTransactionId();
    }

    public XidImpl() {
    }

    public byte[] getBranchQualifier() {
        return branchQualifier;
    }

    public int getFormatId() {
        return formatId;
    }

    public byte[] getGlobalTransactionId() {
        return globalTransactionId;
    }

    public boolean isRouting() {
        return routing;
    }

    public void setRouting(boolean routing) {
        this.routing = routing;
    }

    private boolean bytesEqual(byte[] a, byte[] b) {
        if (a == null && b == null)
            return true;
        if (a == null && b != null ||
                b == null && a != null ||
                a.length != b.length)
            return false;
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i])
                return false;
        }
        return true;
    }

    public boolean equals(Object obj) {
        XidImpl that = (XidImpl) obj;
        return routing == that.isRouting() &&
                bytesEqual(branchQualifier, that.getBranchQualifier()) &&
                bytesEqual(globalTransactionId, that.getGlobalTransactionId()) &&
                formatId == that.getFormatId();
    }

    public void writeContent(DataOutput out) throws IOException {
        out.writeBoolean(routing);
        out.writeInt(formatId);
        if (branchQualifier != null) {
            out.writeByte(1);
            out.writeInt(branchQualifier.length);
            out.write(branchQualifier);
        } else
            out.writeByte(0);
        if (globalTransactionId != null) {
            out.writeByte(1);
            out.writeInt(globalTransactionId.length);
            out.write(globalTransactionId);
        } else
            out.writeByte(0);
    }

    public void readContent(DataInput in) throws IOException {
        routing = in.readBoolean();
        formatId = in.readInt();
        byte set = in.readByte();
        if (set == 1) {
            branchQualifier = new byte[in.readInt()];
            in.readFully(branchQualifier);
        }
        set = in.readByte();
        if (set == 1) {
            globalTransactionId = new byte[in.readInt()];
            in.readFully(globalTransactionId);
        }
    }

    private int createByteHash(byte[] b) {
        int h = 0;
        for (int i = 0; i < b.length; i++)
            h += b[i];
        return h;
    }

    private void createHash() {
        hash = formatId;
        if (branchQualifier != null)
            hash += createByteHash(branchQualifier);
        if (globalTransactionId != null)
            hash += createByteHash(globalTransactionId);
    }

    public int hashCode() {
        if (hash == 0)
            createHash();
        return hash;
    }

    public String toString() {
        StringBuffer b = new StringBuffer("[XidImpl, branchQualifier=");
        b.append(new String(branchQualifier));
        b.append(", formatId=");
        b.append(formatId);
        b.append(", globalTransactionId=");
        b.append(new String(globalTransactionId));
        b.append(", routing=");
        b.append(routing);
        b.append("]");
        return b.toString();
    }
}
