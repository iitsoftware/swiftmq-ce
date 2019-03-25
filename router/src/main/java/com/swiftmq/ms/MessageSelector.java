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

package com.swiftmq.ms;

import com.swiftmq.jms.MessageImpl;
import com.swiftmq.swiftlet.queue.Selector;

import javax.jms.InvalidSelectorException;

// This is a proxy class for compatibility reasons (Swiftlet API)

public class MessageSelector implements Selector {
    com.swiftmq.ms.artemis.MessageSelector instance = null;

    public MessageSelector(String conditionString) {
        instance = new com.swiftmq.ms.artemis.MessageSelector(conditionString);
    }

    public String getConditionString() {
        return instance.getConditionString();
    }

    public void compile() throws InvalidSelectorException {
        instance.compile();
    }

    public boolean isSelected(MessageImpl message) {
        return instance.isSelected(message);
    }

    public String toString() {
        return "[MessageSelector instance = " + instance.toString() + "]";
    }
}
