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

package com.swiftmq.mgmt.protocol.v400;

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.util.SwiftUtilities;

import java.io.*;

public class PropertyChangedRequest extends Request {
    String[] context = null;
    String name = null;
    Object value = null;

    public PropertyChangedRequest(String[] context, String name, Object value) {
        super(0, false);
        this.context = context;
        this.name = name;
        this.value = value;
    }

    public PropertyChangedRequest() {
        this(null, null, null);
    }

    public String[] getContext() {
        return context;
    }

    public void setContext(String[] context) {
        this.context = context;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public int getDumpId() {
        return ProtocolFactory.PROPERTYCHANGED_REQ;
    }

    public void writeContent(DataOutput out)
            throws IOException {
        super.writeContent(out);
        if (context != null) {
            out.writeByte(1);
            out.writeInt(context.length);
            for (int i = 0; i < context.length; i++) {
                out.writeUTF(context[i]);
            }
        } else
            out.writeByte(0);
        out.writeUTF(name);
        if (value != null) {
            out.writeByte(1);
            DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(dbos);
            oos.writeObject(value);
            oos.flush();
            oos.close();
            out.writeInt(dbos.getCount());
            out.write(dbos.getBuffer(), 0, dbos.getCount());
        } else
            out.writeByte(0);
    }

    public void readContent(DataInput in)
            throws IOException {
        super.readContent(in);
        byte set = in.readByte();
        if (set == 1) {
            context = new String[in.readInt()];
            for (int i = 0; i < context.length; i++) {
                context[i] = in.readUTF();
            }
        }
        name = in.readUTF();
        set = in.readByte();
        if (set == 1) {
            byte b[] = new byte[in.readInt()];
            in.readFully(b);
            DataByteArrayInputStream dbis = new DataByteArrayInputStream(b);
            ObjectInputStream ois = new ObjectInputStream(dbis);
            try {
                value = ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException(e.toString());
            }
            ois.close();
        }
    }

    protected Reply createReplyInstance() {
        return null;
    }

    public void accept(RequestVisitor visitor) {
        ((ProtocolVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[PropertyChangedRequest " + super.toString() + ", context=" + (context != null ? SwiftUtilities.concat(context, "/") : "null") +
                ", name=" + name + ", value=" + value + "]";
    }
}
