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

package com.swiftmq.impl.queue.standard.cluster;

import com.swiftmq.impl.queue.standard.SwiftletContext;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.queue.QueueFactory;

public class ClusteredQueueFactory implements QueueFactory {
    SwiftletContext ctx = null;

    public ClusteredQueueFactory(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    public AbstractQueue createQueue(String queueName, Entity clusteredQueueEntity) throws QueueException {
        AbstractQueue queue = new ClusteredQueue(ctx, ctx.dispatchPolicyRegistry.add(queueName, ctx.messageGroupDispatchPolicyFactory.create(ctx, clusteredQueueEntity, queueName, new RoundRobinDispatchPolicy(ctx, queueName))));
        queue.setFlowController(new ClusteredQueueFlowController());
        return queue;
    }
}
