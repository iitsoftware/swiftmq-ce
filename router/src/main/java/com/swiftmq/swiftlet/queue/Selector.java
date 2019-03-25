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

package com.swiftmq.swiftlet.queue;

import com.swiftmq.jms.MessageImpl;

/**
 * Interface for message selectors
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public interface Selector {

    /**
     * Returns the condition string in SQL92 syntax
     *
     * @return condition string
     */
    public String getConditionString();

    /**
     * Returns true if the message matches the selector criteria
     *
     * @param message the message
     * @return true if the message matches
     */
    public boolean isSelected(MessageImpl message);
}

