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

package com.swiftmq.jms.v510;

import com.swiftmq.jms.ExceptionConverter;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.smqp.v510.CreateConsumerRequest;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.RequestRegistry;

import javax.jms.JMSException;
import javax.jms.ServerSessionPool;

public class QueueConnectionConsumerImpl extends ConnectionConsumerImpl {
    String queueName = null;

    public QueueConnectionConsumerImpl(ConnectionImpl myConnection, int dispatchId, RequestRegistry requestRegistry, ServerSessionPool serverSessionPool, int maxMessages) {
        super(myConnection, dispatchId, requestRegistry, serverSessionPool, maxMessages);
    }

    void createConsumer(QueueImpl queue, String messageSelector) throws JMSException {
        if (queue == null)
            throw new NullPointerException("createConsumer, queue is null!");

        queueName = queue.getQueueName();

        Reply reply = null;
        try {
            reply = requestRegistry.request(new CreateConsumerRequest(dispatchId,
                    (QueueImpl) queue,
                    messageSelector));
        } catch (Exception e) {
            throw ExceptionConverter.convert(e);
        }
        if (!reply.isOk())
            throw ExceptionConverter.convert(reply.getException());
        fillCache();
    }

    protected String getQueueName() {
        return queueName;
    }
}
