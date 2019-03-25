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

package com.swiftmq.jms.v750;

import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.smqp.v750.CreateConsumerReply;
import com.swiftmq.jms.smqp.v750.CreateConsumerRequest;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestRegistry;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueReceiver;

public class QueueReceiverImpl extends MessageConsumerImpl implements QueueReceiver {
    Queue queue = null;

    public QueueReceiverImpl(boolean transacted, int acknowledgeMode, RequestRegistry requestRegistry,
                             Queue queue, String messageSelector,
                             SessionImpl session) {
        super(transacted, acknowledgeMode, requestRegistry, messageSelector, session);
        this.queue = queue;
    }

    public Queue getQueue() throws JMSException {
        verifyState();
        return (queue);
    }

    public Request getRecreateRequest() {
        String ms = messageSelector;
        if (messageSelector != null && messageSelector.trim().length() == 0)
            ms = null;
        return new CreateConsumerRequest(mySession, mySession.dispatchId, (QueueImpl) queue, ms);
    }

    public void setRecreateReply(Reply reply) {
        serverQueueConsumerId = ((CreateConsumerReply) reply).getQueueConsumerId();
    }

    public String toString() {
        return queue.toString();
    }
}



