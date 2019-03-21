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

package com.swiftmq.impl.streams.comp.memory.limit;

import com.swiftmq.impl.streams.comp.memory.RetirementCallback;

/**
 * Base interface for Memory Limits.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public interface Limit {

    /**
     * Checks the Memory limits and deletes Messages that doesn't fit in the limit
     */
    public void checkLimit();


    /**
     * Sets a callback which is called for each Message that is retired (removed by a Limit).
     *
     * @param callback Callback
     * @return Limit
     */
    public Limit onRetire(RetirementCallback callback);

    /**
     * Creates a new LimitBuilder and that chains new Limits to the corresponding Memory
     *
     * @return LimitBuilder
     */
    public LimitBuilder limit();

}
