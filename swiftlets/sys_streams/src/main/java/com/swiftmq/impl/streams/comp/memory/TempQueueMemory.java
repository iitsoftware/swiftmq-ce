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

package com.swiftmq.impl.streams.comp.memory;

import com.swiftmq.impl.streams.StreamContext;
import com.swiftmq.swiftlet.queue.QueueException;

import javax.jms.DeliveryMode;

/**
 * Memory implementation that stores all Messages as non-persistent Messages in a Temporary Queue.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class TempQueueMemory extends QueueMemory {
    TempQueueMemory(StreamContext ctx, String name) throws Exception {
        super(ctx, name, ctx.ctx.queueManager.createTemporaryQueue());
    }

    @Override
    protected int getDeliveryMode() {
        return DeliveryMode.NON_PERSISTENT;
    }

    @Override
    String getType() {
        return "TempQueue";
    }

    @Override
    public void deferredClose() {
        super.deferredClose();
        try {
            ctx.ctx.queueManager.deleteTemporaryQueue(getQueueName());
        } catch (QueueException e) {

        }
    }
}
