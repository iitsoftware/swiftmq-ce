/*
 * Copyright 2024 IIT Software GmbH
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

package com.swiftmq.impl.queue.standard.queue;

import com.swiftmq.impl.queue.standard.SwiftletContext;
import com.swiftmq.swiftlet.store.NonPersistentStore;
import com.swiftmq.swiftlet.store.PersistentStore;

import java.util.HashMap;

public class MessageQueueFactoryImpl implements MessageQueueFactory {
    public MessageQueue createMessageQueue(SwiftletContext ctx, String localQueueName, Cache cache, PersistentStore pStore, NonPersistentStore nStore, long cleanUpDelay) {
        cache.setCacheTable(new HashMap(cache.getMaxMessages()));
        return new MessageQueue(ctx, cache, pStore, nStore, cleanUpDelay);
    }
}
