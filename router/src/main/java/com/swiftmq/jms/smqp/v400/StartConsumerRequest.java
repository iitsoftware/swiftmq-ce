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

public class StartConsumerRequest extends Request {
    int queueConsumerId = 0;
    int clientDispatchId = 0;
    int clientListenerId = 0;
    int consumerCacheSize = 0;

    public StartConsumerRequest(int dispatchId, int queueConsumerId,
                                int clientDispatchId, int clientListenerId, int consumerCacheSize) {
        super(dispatchId, false);
        this.queueConsumerId = queueConsumerId;
        this.clientDispatchId = clientDispatchId;
        this.clientListenerId = clientListenerId;
        this.consumerCacheSize = consumerCacheSize;
    }

    public int getDumpId() {
        return SMQPFactory.DID_START_CONSUMER_REQ;
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        out.writeInt(queueConsumerId);
        out.writeInt(clientDispatchId);
        out.writeInt(clientListenerId);
        out.writeInt(consumerCacheSize);
    }

    public void readContent(DataInput in) throws IOException {
        super.readContent(in);

        queueConsumerId = in.readInt();
        clientDispatchId = in.readInt();
        clientListenerId = in.readInt();
        consumerCacheSize = in.readInt();
    }

    protected Reply createReplyInstance() {
        return null;
    }

    public void setQueueConsumerId(int queueConsumerId) {
        this.queueConsumerId = queueConsumerId;
    }

    public int getQueueConsumerId() {
        return (queueConsumerId);
    }

    public void setClientDispatchId(int clientDispatchId) {
        this.clientDispatchId = clientDispatchId;
    }

    public int getClientDispatchId() {
        return (clientDispatchId);
    }

    public void setClientListenerId(int clientListenerId) {
        this.clientListenerId = clientListenerId;
    }

    public int getClientListenerId() {
        return (clientListenerId);
    }

    public int getConsumerCacheSize() {
        return consumerCacheSize;
    }

    public void accept(RequestVisitor visitor) {
        ((SMQPVisitor) visitor).visitStartConsumerRequest(this);
    }

    public String toString() {
        return "[StartConsumerRequest " + super.toString()
                + " queueConsumerId=" + queueConsumerId + " clientDispatchId="
                + clientDispatchId + " clientListenerId=" + clientListenerId + " consumerCacheSize=" + consumerCacheSize + "]";
    }

}



