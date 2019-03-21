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

/**
 * Callback that is registered at a MemoryGroup. It is called AFTER a Memory has
 * been removed from a MemoryGroup due to a GroupInactivityTimeout
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2017, All Rights Reserved
 */
public interface MemoryRemoveCallback {
    /**
     * Called from a MemoryGroup AFTER a Memory has been removed from the Group.
     * The Memory already has been closed and removed from the Stream. This callback
     * is to cleanup resources, e.g. to delete persistent queues for QueueMemories.
     *
     * @param key Value of the group Property
     */
    public void onRemove(Comparable key);
}
