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

import com.swiftmq.jms.TopicImpl;
import com.swiftmq.jms.smqp.v750.CreateDurableReply;
import com.swiftmq.jms.smqp.v750.CreateDurableRequest;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestRegistry;

import javax.jms.Topic;

public class DurableTopicSubscriberImpl extends TopicSubscriberImpl {
    String durableName = null;

    public DurableTopicSubscriberImpl(boolean transacted, int acknowledgeMode, RequestRegistry requestRegistry, Topic topic, String messageSelector, SessionImpl session, boolean noLocal, String durableName) {
        super(transacted, acknowledgeMode, requestRegistry, topic, messageSelector, session, noLocal);
        this.durableName = durableName;
    }

    public Request getRecreateRequest() {
        String ms = messageSelector;
        if (messageSelector != null && messageSelector.trim().length() == 0)
            ms = null;
        return new CreateDurableRequest(mySession.dispatchId, (TopicImpl) topic, ms, noLocal, durableName);
    }

    public void setRecreateReply(Reply reply) {
        serverQueueConsumerId = ((CreateDurableReply) reply).getTopicSubscriberId();
    }

    @Override
    public String toString() {
        return "DurableTopicSubscriberImpl {" +
                "durableName='" + durableName + '\'' +
                '}';
    }
}
