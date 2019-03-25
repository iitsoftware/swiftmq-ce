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

package com.swiftmq.jms.smqp.v500;

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GetAuthChallengeRequest extends Request {
    String userName = null;

    /**
     * @param userName
     * @SBGen Constructor assigns userName
     */
    public GetAuthChallengeRequest(String userName) {
        super(0, true);

        // SBgen: Assign variable
        this.userName = userName;
    }

    /**
     * Returns a unique dump id for this object.
     *
     * @return unique dump id
     */
    public int getDumpId() {
        return SMQPFactory.DID_GET_AUTH_CHALLENGE_REQ;
    }

    /**
     * Write the content of this object to the stream.
     *
     * @param out output stream
     * @throws IOException if an error occurs
     */
    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);

        if (userName == null) {
            out.writeByte(0);
        } else {
            out.writeByte(1);
            out.writeUTF(userName);
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
            userName = null;
        } else {
            userName = in.readUTF();
        }
    }


    /**
     * @return
     */
    protected Reply createReplyInstance() {
        return new GetAuthChallengeReply();
    }

    /**
     * @return
     * @SBGen Method get userName
     */
    public String getUserName() {
        // SBgen: Get variable
        return (userName);
    }

    public void accept(RequestVisitor visitor) {
        ((SMQPVisitor) visitor).visitGetAuthChallengeRequest(this);
    }

    public String toString() {
        return "[GetAuthChallengeRequest " + super.toString() + " userName="
                + userName + "]";
    }
}

