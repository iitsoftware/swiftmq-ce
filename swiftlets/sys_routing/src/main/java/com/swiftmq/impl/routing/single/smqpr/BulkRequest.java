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

package com.swiftmq.impl.routing.single.smqpr;

import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.DumpableFactory;
import com.swiftmq.tools.dump.Dumpalizer;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BulkRequest extends Request {
    DumpableFactory factory = null;

    public Object[] dumpables = null;
    public int len = 0;

    public BulkRequest(DumpableFactory factory) {
        super(0, false);
        this.factory = factory;
    }

    public BulkRequest() {
        this(null);
    }

    public void writeContent(DataOutput out)
            throws IOException {
        super.writeContent(out);
        out.writeInt(len);
        for (int i = 0; i < len; i++)
            Dumpalizer.dump(out, (Dumpable) dumpables[i]);
    }

    public void readContent(DataInput in)
            throws IOException {
        super.readContent(in);
        len = in.readInt();
        if (dumpables == null || dumpables.length < len)
            dumpables = new Object[len];
        for (int i = 0; i < len; i++) {
            dumpables[i] = Dumpalizer.construct(in, factory);
        }
    }

    public int getDumpId() {
        return SMQRFactory.BULK_REQ;
    }

    protected Reply createReplyInstance() {
        return null;
    }

    public void accept(RequestVisitor visitor) {
    }

    private String dumpDumpables() {
        StringBuffer b = new StringBuffer("\n");
        for (int i = 0; i < len; i++) {
            b.append(dumpables[i].toString());
            b.append("\n");
        }
        return b.toString();
    }

    public String toString() {
        return "[BulkRequest " + super.toString() +
                " len =" + len +
                " dumpables=" + dumpDumpables() + "]";
    }
}
