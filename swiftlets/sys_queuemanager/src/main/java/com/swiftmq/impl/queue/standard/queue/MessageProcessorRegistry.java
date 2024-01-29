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

import com.swiftmq.jms.MessageImpl;
import com.swiftmq.swiftlet.queue.MessageProcessor;
import com.swiftmq.swiftlet.queue.QueueTimeoutException;
import com.swiftmq.swiftlet.queue.Selector;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class MessageProcessorRegistry {
    private List<MessageProcessor>[] messageProcessors = null;
    private int activeListIndex;

    @SuppressWarnings("unchecked")
    public MessageProcessorRegistry() {
        reset();
    }

    public int storeMessageProcessor(MessageProcessor processor) {
        int regId = processor.getRegistrationId();
        if (regId >= 0 && regId < messageProcessors[activeListIndex].size() && messageProcessors[activeListIndex].get(regId) == processor) {
            return regId;
        }

        messageProcessors[activeListIndex].add(processor);
        regId = messageProcessors[activeListIndex].size() - 1;
        processor.setRegistrationId(regId);
        return regId;
    }

    public void removeMessageProcessor(MessageProcessor processor) {
        int id = processor.getRegistrationId();
        if (id != -1 && id < messageProcessors[activeListIndex].size()) {
            messageProcessors[activeListIndex].set(id, null);
            processor.setRegistrationId(-1);
        }
    }

    public void removeIf(Predicate<MessageProcessor> condition) {
        List<MessageProcessor> currentProcessors = messageProcessors[activeListIndex];
        for (int i = 0; i < currentProcessors.size(); i++) {
            MessageProcessor processor = currentProcessors.get(i);
            if (processor != null && condition.test(processor)) {
                currentProcessors.set(i, null);
                processor.setRegistrationId(-1);
                processor.processException(new QueueTimeoutException("timout occurred"));
                break;
            }
        }
    }

    public boolean hasInterestedProcessor(MessageImpl message) {
        for (MessageProcessor mp : messageProcessors[activeListIndex]) {
            if (mp != null) {
                Selector selector = mp.getSelector();
                if (selector == null || selector.isSelected(message)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void switchProcessorList() {
        activeListIndex = (activeListIndex + 1) % messageProcessors.length;
        messageProcessors[activeListIndex] = new ArrayList<>();
    }

    public void process(Consumer<MessageProcessor> processorConsumer) {
        List<MessageProcessor> currentProcessors = messageProcessors[activeListIndex];
        if (currentProcessors.isEmpty())
            return;
        messageProcessors[activeListIndex] = new ArrayList<>(); // Assign a new ArrayList

        for (MessageProcessor processor : currentProcessors) {
            if (processor != null) {
                processorConsumer.accept(processor);
            }
        }
    }

    public void reset() {
        // Initialize the array with two ArrayLists for round-robin processing
        messageProcessors = new ArrayList[2];
        messageProcessors[0] = new ArrayList<>();
        messageProcessors[1] = new ArrayList<>();
        activeListIndex = 0;
    }
}
