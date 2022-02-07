/*
 * Copyright 2022 IIT Software GmbH
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

package com.swiftmq.impl.queue.standard;

import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.swiftlet.queue.MessageIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ViewCollector {
    private final AbstractQueue abstractQueue;
    private final int from;
    private List<MessageIndex> collected = null;
    private int viewId = -1;
    private int index = 0;

    public ViewCollector(AbstractQueue abstractQueue, int from, MessageSelector selector, int max) throws Exception {
        this.abstractQueue = abstractQueue;
        this.from = from;
        Set<MessageIndex> queueContent;
        if (selector != null) {
            this.viewId = abstractQueue.createView(selector);
            queueContent = abstractQueue.getQueueIndex(this.viewId);
        } else
            queueContent = abstractQueue.getQueueIndex();
        if (from < queueContent.size())
            collected = new ArrayList<>(queueContent).subList(from, Math.min(from + max, queueContent.size()));
        else
            collected = new ArrayList<>();
    }

    public int resultSize() {
        return collected.size();
    }

    public long queueSize() throws Exception {
        return abstractQueue.getNumberQueueMessages();
    }

    public ViewEntry next() throws Exception {
        ViewEntry viewEntry = null;
        MessageEntry messageEntry = null;
        for (int i = index; i < collected.size(); i++) {
            messageEntry = abstractQueue.getMessageByIndex(collected.get(i));
            if (messageEntry != null) {
                viewEntry = new ViewEntry(messageEntry, from + i);
                index = i + 1;
                break;
            }
        }
        return viewEntry;
    }

    public void close() {
        if (viewId != -1)
            this.abstractQueue.deleteView(viewId);
    }
}
