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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SetClientIdReply extends Reply {
    String clientId = null;

    /**
     * Returns a unique dump id for this object.
     *
     * @return unique dump id
     */
    public int getDumpId() {
        return SMQPFactory.DID_SET_CLIENT_ID_REP;
    }

    /**
     * Write the content of this object to the stream.
     *
     * @param out output stream
     * @throws IOException if an error occurs
     */
    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);

        if (clientId == null) {
            out.writeByte(0);
        } else {
            out.writeByte(1);
            out.writeUTF(clientId);
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
            clientId = null;
        } else {
            clientId = in.readUTF();
        }
    }

    /**
     * @param clientId
     * @SBGen Method set clientId
     */
    public void setClientId(String clientId) {

        // SBgen: Assign variable
        this.clientId = clientId;
    }

    /**
     * @return
     * @SBGen Method get clientId
     */
    public String getClientId() {

        // SBgen: Get variable
        return (clientId);
    }

    /**
     * Method declaration
     *
     * @return
     * @see
     */
    public String toString() {
        return "[SetClientIdReply " + super.toString() + " clientId=" + clientId
                + "]";
    }
}

