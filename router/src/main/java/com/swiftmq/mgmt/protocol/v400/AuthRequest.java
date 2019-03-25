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

import java.io.*;

public class AuthRequest extends Request {
    Serializable response = null;

    AuthRequest() {
        this(null);
    }

    public AuthRequest(Serializable response) {
        super(0, true);
        this.response = response;
    }

    public int getDumpId() {
        return ProtocolFactory.AUTH_REQ;
    }

    public void writeContent(DataOutput output) throws IOException {
        super.writeContent(output);
        DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(dbos);
        oos.writeObject(response);
        oos.flush();
        oos.close();
        output.writeInt(dbos.getCount());
        output.write(dbos.getBuffer(), 0, dbos.getCount());
    }

    public void readContent(DataInput input) throws IOException {
        super.readContent(input);
        byte b[] = new byte[input.readInt()];
        input.readFully(b);
        DataByteArrayInputStream dbis = new DataByteArrayInputStream(b);
        ObjectInputStream ois = new ObjectInputStream(dbis);
        try {
            response = (Serializable) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(e.toString());
        }
        ois.close();
    }

    public Serializable getResponse() {
        return response;
    }

    public void setResponse(Serializable response) {
        this.response = response;
    }

    protected Reply createReplyInstance() {
        return new AuthReply();
    }

    public void accept(RequestVisitor visitor) {
        ((ProtocolVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[AuthRequest " + super.toString() + ", response=" + response + "]";
    }
}
