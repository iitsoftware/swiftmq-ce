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
    private List<MessageProcessor> messageProcessors = new ArrayList<>();

    @SuppressWarnings("unchecked")
    public MessageProcessorRegistry() {
        reset();
    }

    public int storeMessageProcessor(MessageProcessor processor) {
        int regId = processor.getRegistrationId();
        if (regId >= 0 && regId < messageProcessors.size() && messageProcessors.get(regId) == processor) {
            return regId;
        }

        messageProcessors.add(processor);
        regId = messageProcessors.size() - 1;
        processor.setRegistrationId(regId);
        return regId;
    }

    public void removeMessageProcessor(MessageProcessor processor) {
        int id = processor.getRegistrationId();
        if (id != -1 && id < messageProcessors.size()) {
            messageProcessors.set(id, null);
            processor.setRegistrationId(-1);
        }
    }

    public void removeIf(Predicate<MessageProcessor> condition) {
        for (int i = 0; i < messageProcessors.size(); i++) {
            MessageProcessor processor = messageProcessors.get(i);
            if (processor != null && condition.test(processor)) {
                messageProcessors.set(i, null);
                processor.setRegistrationId(-1);
                processor.processException(new QueueTimeoutException("timout occurred"));
                break;
            }
        }
    }

    public boolean hasInterestedProcessor(MessageImpl message) {
        for (MessageProcessor mp : messageProcessors) {
            if (mp != null) {
                Selector selector = mp.getSelector();
                if (selector == null || selector.isSelected(message)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void process(Consumer<MessageProcessor> processorConsumer) {
        // Necessary copy and reset as the processors might register again during the call
        List<MessageProcessor> list = new ArrayList<>(messageProcessors);
        reset();
        for (MessageProcessor processor : list) {
            if (processor != null) {
                processor.setRegistrationId(-1);
                processorConsumer.accept(processor);
            }
        }
    }

    public void reset() {
        messageProcessors = new ArrayList<>();
    }
}
