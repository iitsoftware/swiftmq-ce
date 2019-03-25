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

package com.swiftmq.jms.smqp.v750;

/**
 * SMQP-Protocol Version 750, Class: CreateShadowConsumerReply
 * Automatically generated, don't change!
 * Generation Date: Tue Apr 21 10:39:21 CEST 2009
 * (c) 2009, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.tools.requestreply.ReplyNE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CreateShadowConsumerReply extends ReplyNE {
    private int queueConsumerId;

    public CreateShadowConsumerReply(int queueConsumerId) {
        this.queueConsumerId = queueConsumerId;
    }

    protected CreateShadowConsumerReply() {
    }

    public void setQueueConsumerId(int queueConsumerId) {
        this.queueConsumerId = queueConsumerId;
    }

    public int getQueueConsumerId() {
        return queueConsumerId;
    }

    public int getDumpId() {
        return SMQPFactory.DID_CREATESHADOWCONSUMER_REP;
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        SMQPUtil.write(queueConsumerId, out);
    }

    public void readContent(DataInput in) throws IOException {
        super.readContent(in);
        queueConsumerId = SMQPUtil.read(queueConsumerId, in);
    }

    public String toString() {
        StringBuffer _b = new StringBuffer("[v750/CreateShadowConsumerReply, ");
        _b.append(super.toString());
        _b.append(", ");
        _b.append("queueConsumerId=");
        _b.append(queueConsumerId);
        _b.append("]");
        return _b.toString();
    }
}
