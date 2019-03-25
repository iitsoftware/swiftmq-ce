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

package com.swiftmq.jms;

import com.swiftmq.jms.v750.SessionImpl;

import javax.jms.JMSException;
import javax.jms.TemporaryQueue;

public class TemporaryQueueImpl extends QueueImpl implements TemporaryQueue {
    transient SwiftMQConnection connection = null;
    transient SessionImpl creatingSession = null;

    public TemporaryQueueImpl(String queueName, SwiftMQConnection connection) {
        super(queueName);

        this.connection = connection;
    }

    public int getType() {
        return DestinationFactory.TYPE_TEMPQUEUE;
    }

    public void setCreatingSession(SessionImpl creatingSession) {
        this.creatingSession = creatingSession;
    }

    public boolean isCreatungSession(SessionImpl session) {
        return creatingSession == null || creatingSession == session;
    }

    public void delete() throws JMSException {
        if (connection == null)
            throw new JMSException("Cannot delete; you are not the creator of this TemporaryQueue!");
        connection.deleteTempQueue(getQueueName());
    }

}



