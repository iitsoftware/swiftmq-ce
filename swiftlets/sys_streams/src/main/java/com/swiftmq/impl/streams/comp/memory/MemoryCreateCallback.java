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
 * Callback that is registered at a MemoryGroup. It is called to create a
 * new Memory for a new value of the group Property.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2017, All Rights Reserved
 */
public interface MemoryCreateCallback {
    /**
     * Create a new Memory
     *
     * @param key Value of the Group Property
     * @return Memory or null if the value should not be respected
     * @throws Exception
     */
    public Memory create(Comparable key) throws Exception;
}
