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

package com.swiftmq.jms.smqp.v510;

/**
 * SMQP-Protocol Version 510, Class: StartConsumerRequest
 * Automatically generated, don't change!
 * Generation Date: Fri Aug 13 16:00:44 CEST 2004
 * (c) 2004, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StartConsumerRequest extends Request {
    private int queueConsumerId;
    private int clientDispatchId;
    private int clientListenerId;
    private int consumerCacheSize;

    public StartConsumerRequest() {
        super(0, false);
    }

    public StartConsumerRequest(int dispatchId) {
        super(dispatchId, false);
    }

    public StartConsumerRequest(int dispatchId, int queueConsumerId, int clientDispatchId, int clientListenerId, int consumerCacheSize) {
        super(dispatchId, false);
        this.queueConsumerId = queueConsumerId;
        this.clientDispatchId = clientDispatchId;
        this.clientListenerId = clientListenerId;
        this.consumerCacheSize = consumerCacheSize;
    }

    public void setQueueConsumerId(int queueConsumerId) {
        this.queueConsumerId = queueConsumerId;
    }

    public int getQueueConsumerId() {
        return queueConsumerId;
    }

    public void setClientDispatchId(int clientDispatchId) {
        this.clientDispatchId = clientDispatchId;
    }

    public int getClientDispatchId() {
        return clientDispatchId;
    }

    public void setClientListenerId(int clientListenerId) {
        this.clientListenerId = clientListenerId;
    }

    public int getClientListenerId() {
        return clientListenerId;
    }

    public void setConsumerCacheSize(int consumerCacheSize) {
        this.consumerCacheSize = consumerCacheSize;
    }

    public int getConsumerCacheSize() {
        return consumerCacheSize;
    }

    public int getDumpId() {
        return SMQPFactory.DID_STARTCONSUMER_REQ;
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        SMQPUtil.write(queueConsumerId, out);
        SMQPUtil.write(clientDispatchId, out);
        SMQPUtil.write(clientListenerId, out);
        SMQPUtil.write(consumerCacheSize, out);
    }

    public void readContent(DataInput in) throws IOException {
        super.readContent(in);
        queueConsumerId = SMQPUtil.read(queueConsumerId, in);
        clientDispatchId = SMQPUtil.read(clientDispatchId, in);
        clientListenerId = SMQPUtil.read(clientListenerId, in);
        consumerCacheSize = SMQPUtil.read(consumerCacheSize, in);
    }

    protected Reply createReplyInstance() {
        return null;
    }

    public void accept(RequestVisitor visitor) {
        ((SMQPVisitor) visitor).visit(this);
    }

    public String toString() {
        StringBuffer _b = new StringBuffer("[StartConsumerRequest, ");
        _b.append(super.toString());
        _b.append(", ");
        _b.append("queueConsumerId=");
        _b.append(queueConsumerId);
        _b.append(", ");
        _b.append("clientDispatchId=");
        _b.append(clientDispatchId);
        _b.append(", ");
        _b.append("clientListenerId=");
        _b.append(clientListenerId);
        _b.append(", ");
        _b.append("consumerCacheSize=");
        _b.append(consumerCacheSize);
        _b.append("]");
        return _b.toString();
    }
}
