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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CreateSubscriberReply extends Reply {
    String tmpQueueName = null;
    int topicSubscriberId = 0;

    public int getDumpId() {
        return SMQPFactory.DID_CREATE_SUBSCRIBER_REP;
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        out.writeInt(topicSubscriberId);
        if (tmpQueueName != null) {
            out.writeByte(1);
            out.writeUTF(tmpQueueName);
        } else
            out.write(0);
    }

    public void readContent(DataInput in) throws IOException {
        super.readContent(in);
        topicSubscriberId = in.readInt();
        if (in.readByte() == 1)
            tmpQueueName = in.readUTF();
    }

    public String getTmpQueueName() {
        return tmpQueueName;
    }

    public void setTmpQueueName(String tmpQueueName) {
        this.tmpQueueName = tmpQueueName;
    }

    public void setTopicSubscriberId(int topicSubscriberId) {
        this.topicSubscriberId = topicSubscriberId;
    }

    public int getTopicSubscriberId() {
        return (topicSubscriberId);
    }

    public String toString() {
        return "[CreateSubscriberReply " + super.toString() + " topicSubscriberId="
                + topicSubscriberId + " tmpQueueName=" + tmpQueueName + "]";
    }

}



