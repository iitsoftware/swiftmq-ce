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

/**
 * @author Andreas Mueller, IIT GmbH
 * @version 1.0
 */
public class DeleteDurableRequest extends Request {
    String durableName;

    /**
     * @param dispatchId
     * @param durableName
     * @SBGen Constructor assigns dispatchId, durableName
     */
    public DeleteDurableRequest(int dispatchId, String durableName) {
        super(dispatchId, true);

        // SBgen: Assign variable
        this.durableName = durableName;
    }

    /**
     * Returns a unique dump id for this object.
     *
     * @return unique dump id
     */
    public int getDumpId() {
        return SMQPFactory.DID_DELETE_DURABLE_REQ;
    }

    /**
     * Write the content of this object to the stream.
     *
     * @param out output stream
     * @throws IOException if an error occurs
     */
    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);

        if (durableName == null) {
            out.writeByte(0);
        } else {
            out.writeByte(1);
            out.writeUTF(durableName);
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
            durableName = null;
        } else {
            durableName = in.readUTF();
        }
    }

    /**
     * @return
     */
    protected Reply createReplyInstance() {
        return new DeleteDurableReply();
    }

    /**
     * @param durableName
     * @SBGen Method set durableName
     */
    public void setDurableName(String durableName) {

        // SBgen: Assign variable
        this.durableName = durableName;
    }

    /**
     * @return
     * @SBGen Method get durableName
     */
    public String getDurableName() {

        // SBgen: Get variable
        return (durableName);
    }

    public void accept(RequestVisitor visitor) {
        ((SMQPVisitor) visitor).visitDeleteDurableRequest(this);
    }

    /**
     * Method declaration
     *
     * @return
     * @see
     */
    public String toString() {
        return "[DeleteDurableRequest " + super.toString() + " durableName="
                + durableName + "]";
    }

}

