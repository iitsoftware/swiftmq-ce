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

package com.swiftmq.jms.smqp.v630;

/**
 * SMQP-Protocol Version 630, Class: CreateDurableReply
 * Automatically generated, don't change!
 * Generation Date: Thu Aug 30 17:17:54 CEST 2007
 * (c) 2007, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.tools.requestreply.ReplyNE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CreateDurableReply extends ReplyNE {
    private String queueName;
    private int topicSubscriberId;

    public CreateDurableReply(String queueName, int topicSubscriberId) {
        this.queueName = queueName;
        this.topicSubscriberId = topicSubscriberId;
    }

    protected CreateDurableReply() {
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setTopicSubscriberId(int topicSubscriberId) {
        this.topicSubscriberId = topicSubscriberId;
    }

    public int getTopicSubscriberId() {
        return topicSubscriberId;
    }

    public int getDumpId() {
        return SMQPFactory.DID_CREATEDURABLE_REP;
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        if (queueName != null) {
            out.writeBoolean(true);
            SMQPUtil.write(queueName, out);
        } else
            out.writeBoolean(false);
        SMQPUtil.write(topicSubscriberId, out);
    }

    public void readContent(DataInput in) throws IOException {
        super.readContent(in);
        boolean queueName_set = in.readBoolean();
        if (queueName_set)
            queueName = SMQPUtil.read(queueName, in);
        topicSubscriberId = SMQPUtil.read(topicSubscriberId, in);
    }

    public String toString() {
        StringBuffer _b = new StringBuffer("[v630/CreateDurableReply, ");
        _b.append(super.toString());
        _b.append(", ");
        _b.append("queueName=");
        _b.append(queueName);
        _b.append(", ");
        _b.append("topicSubscriberId=");
        _b.append(topicSubscriberId);
        _b.append("]");
        return _b.toString();
    }
}
