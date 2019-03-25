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

package com.swiftmq.swiftlet.monitor;

import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.queue.QueueManager;

import java.util.Map;
import java.util.TreeMap;

public class QueueCollector implements Collector {
    SwiftletContext ctx = null;
    QueueManager queueManager = null;

    public QueueCollector(SwiftletContext ctx) {
        this.ctx = ctx;
        queueManager = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
    }

    public String getDescription() {
        return "QUEUE COLLECTOR";
    }

    public String[] getColumnNames() {
        return new String[]{"Queue Name", "Number Messages"};
    }

    public Map collect() {
        Map map = new TreeMap();
        String[] names = queueManager.getDefinedQueueNames();
        if (names != null) {
            for (int i = 0; i < names.length; i++) {
                if (!names[i].startsWith("tpc$")) {
                    AbstractQueue queue = queueManager.getQueueForInternalUse(names[i]);
                    if (queue != null) {
                        try {
                            map.put(names[i], new Long(queue.getNumberQueueMessages()));
                        } catch (QueueException e) {
                        }
                    }
                }
            }
        }
        return map;
    }
}
