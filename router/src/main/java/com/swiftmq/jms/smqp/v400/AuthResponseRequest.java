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

package com.swiftmq.jms.smqp.v400;

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.*;

public class AuthResponseRequest extends Request {
    Serializable response = null;

    /**
     * @param response
     * @SBGen Constructor assigns response
     */
    public AuthResponseRequest(Serializable response) {
        super(0, true);

        // SBgen: Assign variable
        this.response = response;
    }

    /**
     * Returns a unique dump id for this object.
     *
     * @return unique dump id
     */
    public int getDumpId() {
        return SMQPFactory.DID_AUTH_RESPONSE_REQ;
    }

    /**
     * Write the content of this object to the stream.
     *
     * @param out output stream
     * @throws IOException if an error occurs
     */
    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);

        if (response == null) {
            out.writeByte(0);
        } else {
            out.writeByte(1);
            DataByteArrayOutputStream dos = new DataByteArrayOutputStream(256);
            (new ObjectOutputStream(dos)).writeObject(response);
            out.writeInt(dos.getCount());
            out.write(dos.getBuffer(), 0, dos.getCount());
        }
    }

    /**
     * Read the content of this object from the stream.
     *
     * @param in input stream
     * @throws IOException if an error occurs
     */
    public void readContent(DataInput in) throws IOException {
        super.readContent(in);

        byte set = in.readByte();

        if (set == 0) {
            response = null;
        } else {
            try {
                byte[] b = new byte[in.readInt()];
                in.readFully(b);
                response = (Serializable) (new ObjectInputStream(new DataByteArrayInputStream(b))).readObject();
            } catch (ClassNotFoundException ignored) {
            }
        }
    }

    public Serializable getResponse() {
        return response;
    }

    /**
     * @return
     */
    protected Reply createReplyInstance() {
        return new AuthResponseReply();
    }


    public void accept(RequestVisitor visitor) {
        ((SMQPVisitor) visitor).visitAuthResponseRequest(this);
    }

    public String toString() {
        return "[AuthResponseRequest " + super.toString() + " response="
                + response + "]";
    }
}

