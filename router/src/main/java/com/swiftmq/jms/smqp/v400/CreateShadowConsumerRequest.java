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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CreateShadowConsumerRequest extends Request {
    String queueName = null;

    public CreateShadowConsumerRequest(int dispatchId, String queueName) {
        super(dispatchId, true);
        this.queueName = queueName;
    }

    public int getDumpId() {
        return SMQPFactory.DID_CREATE_SHADOW_CONSUMER_REQ;
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);

        if (queueName == null) {
            out.writeByte(0);
        } else {
            out.writeByte(1);
            out.writeUTF(queueName);
        }
    }

    public void readContent(DataInput in) throws IOException {
        super.readContent(in);

        byte set = in.readByte();

        if (set == 0) {
            queueName = null;
        } else {
            queueName = in.readUTF();
        }
    }

    protected Reply createReplyInstance() {
        return new CreateShadowConsumerReply();
    }

    public String getQueueName() {
        return (queueName);
    }

    public void accept(RequestVisitor visitor) {
        ((SMQPVisitor) visitor).visitCreateShadowConsumerRequest(this);
    }

    public String toString() {
        return "[CreateShadowConsumerRequest " + super.toString() + " queueName=" + queueName + "]";
    }

}
