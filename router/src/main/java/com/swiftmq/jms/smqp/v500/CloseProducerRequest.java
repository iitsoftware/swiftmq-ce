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

/*--- formatted by Jindent 2.1, (www.c-lab.de/~jindent) ---*/

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
public class CloseProducerRequest extends Request {
    int queueProducerId = -1;

    /**
     * @param dispatchId
     * @SBGen Constructor
     */
    public CloseProducerRequest(int dispatchId, int queueProducerId) {
        super(dispatchId, true);
        this.queueProducerId = queueProducerId;
    }

    /**
     * Returns a unique dump id for this object.
     *
     * @return unique dump id
     */
    public int getDumpId() {
        return SMQPFactory.DID_CLOSE_PRODUCER_REQ;
    }

    /**
     * Write the content of this object to the stream.
     *
     * @param out output stream
     * @throws IOException if an error occurs
     */
    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        out.writeInt(queueProducerId);
    }

    /**
     * Read the content of this object from the stream.
     *
     * @param in input stream
     * @throws IOException if an error occurs
     */
    public void readContent(DataInput in) throws IOException {
        super.readContent(in);

        queueProducerId = in.readInt();
    }

    public int getQueueProducerId() {
        return queueProducerId;
    }

    /**
     * @return
     */
    protected Reply createReplyInstance() {
        return new CloseProducerReply();
    }

    public void accept(RequestVisitor visitor) {
        ((SMQPVisitor) visitor).visitCloseProducerRequest(this);
    }

    /**
     * Method declaration
     *
     * @return
     * @see
     */
    public String toString() {
        return "[CloseProducerRequest " + super.toString() + ", queueProducerId=" + queueProducerId + "]";
    }

}



