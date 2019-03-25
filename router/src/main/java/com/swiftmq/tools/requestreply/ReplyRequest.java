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

package com.swiftmq.tools.requestreply;

import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.*;

public abstract class ReplyRequest extends Request {
    boolean ok = false;
    Exception exception = null;

    public ReplyRequest(int dispatchId, boolean replyRequired) {
        super(dispatchId, replyRequired);
    }

    /**
     * Write the content of this object to the stream.
     *
     * @param out output stream
     * @throws IOException if an error occurs
     */
    public void writeContent(DataOutput out)
            throws IOException {
        out.writeBoolean(ok);
        if (exception == null)
            out.writeByte(0);
        else {
            out.writeByte(1);
            DataByteArrayOutputStream dos = new DataByteArrayOutputStream(256);
            (new ObjectOutputStream(dos)).writeObject(exception);
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
    public void readContent(DataInput in)
            throws IOException {
        ok = in.readBoolean();
        byte set = in.readByte();
        if (set == 0)
            exception = null;
        else {
            try {
                byte[] b = new byte[in.readInt()];
                in.readFully(b);
                exception = (Exception) (new ObjectInputStream(new DataByteArrayInputStream(b))).readObject();
            } catch (ClassNotFoundException ignored) {
            }
        }
    }

    public boolean isOk() {
        return ok;
    }

    public void setOk(boolean ok) {
        this.ok = ok;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public String toString() {
        return "[ReplyRequest, ok=" + ok + " exception=" + exception + " ]";
    }
}
