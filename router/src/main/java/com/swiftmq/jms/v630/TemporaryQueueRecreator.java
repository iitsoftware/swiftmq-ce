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

package com.swiftmq.jms.v630;

import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.smqp.v630.CreateTmpQueueReply;
import com.swiftmq.jms.smqp.v630.CreateTmpQueueRequest;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;

import javax.jms.JMSException;
import java.util.List;

public class TemporaryQueueRecreator implements Recreatable {
    ConnectionImpl connection = null;
    QueueImpl tempQueue = null;

    public TemporaryQueueRecreator(ConnectionImpl connection, QueueImpl tempQueue) {
        this.connection = connection;
        this.tempQueue = tempQueue;
    }

    public Request getRecreateRequest() {
        return new CreateTmpQueueRequest();
    }

    public void setRecreateReply(Reply reply) {
        try {
            connection.removeTmpQueue(tempQueue.getQueueName());
        } catch (JMSException e) {
        }
        tempQueue.setQueueName(((CreateTmpQueueReply) reply).getQueueName());
        connection.addTmpQueue(tempQueue);
    }

    public List getRecreatables() {
        return null;
    }

    public String toString() {
        return "TemporaryQueueRecreator, tempQueue=" + tempQueue;
    }
}
