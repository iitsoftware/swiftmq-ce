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

package com.swiftmq.impl.jms.standard.v500;

import com.swiftmq.jms.DestinationFactory;
import com.swiftmq.jms.TopicImpl;

public class TopicProducer extends Producer {
    TopicImpl topic;

    protected TopicProducer(SessionContext ctx, TopicImpl topic)
            throws Exception {
        super(ctx);
        this.topic = topic;
        if (topic.getType() != DestinationFactory.TYPE_TEMPTOPIC)
            this.topic = ctx.topicManager.verifyTopic(topic);
        String queueName = topic.getQueueName();
        setQueueSender(ctx.queueManager.createQueueSender(queueName, ctx.activeLogin));
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/created");
    }

    public String toString() {
        return "TopicProducer, topic=" + topic;
    }
}

