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

package com.swiftmq.impl.streams.comp.io;

import com.swiftmq.impl.streams.StreamContext;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.swiftlet.queue.QueueSender;

import javax.jms.Destination;

/**
 * Sends Messages to a Topic.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class TopicOutput extends Output {

    TopicImpl destination;

    TopicOutput(StreamContext ctx, String name) throws Exception {
        super(ctx, name);
    }

    @Override
    protected void setDestination(Destination destination) {
        this.destination = (TopicImpl) destination;
    }

    @Override
    protected String getType() {
        return "Topic";
    }

    @Override
    protected QueueSender createSender() throws Exception {
        return ctx.ctx.queueManager.createQueueSender(((TopicImpl) getDestination()).getQueueName(), null);
    }

    @Override
    protected Destination getDestination() throws Exception {
        if (destination != null)
            return destination;
        return ctx.ctx.topicManager.verifyTopic(new TopicImpl(name));
    }
}
