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

import com.swiftmq.jms.QueueImpl;
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
public class CreateProducerRequest extends Request {
    QueueImpl queue = null;

    /**
     * @param queue
     * @param dispatchId
     * @SBGen Constructor assigns queue
     */
    public CreateProducerRequest(int dispatchId, QueueImpl queue) {
        super(dispatchId, true);

        // SBgen: Assign variable
        this.queue = queue;
    }

    /**
     * Returns a unique dump id for this object.
     *
     * @return unique dump id
     */
    public int getDumpId() {
        return SMQPFactory.DID_CREATE_PRODUCER_REQ;
    }

    /**
     * Write the content of this object to the stream.
     *
     * @param out output stream
     * @throws IOException if an error occurs
     */
    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);

        if (queue == null) {
            out.writeByte(0);
        } else {
            out.writeByte(1);
            out.writeUTF(queue.toString());
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
            queue = null;
        } else {
            queue = new QueueImpl(in.readUTF());
        }
    }

    /**
     * @return
     */
    protected Reply createReplyInstance() {
        return new CreateProducerReply();
    }

    /**
     * @param queue
     * @SBGen Method set queue
     */
    public void setQueue(QueueImpl queue) {

        // SBgen: Assign variable
        this.queue = queue;
    }

    /**
     * @return
     * @SBGen Method get queue
     */
    public QueueImpl getQueue() {

        // SBgen: Get variable
        return (queue);
    }

    public void accept(RequestVisitor visitor) {
        ((SMQPVisitor) visitor).visitCreateProducerRequest(this);
    }

    /**
     * Method declaration
     *
     * @return
     * @see
     */
    public String toString() {
        return "[CreateProducerRequest " + super.toString() + " queue=" + queue
                + "]";
    }

}



