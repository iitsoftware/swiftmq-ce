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

public class QueueLatency {
    private long cumulated = 0;
    private int nMessages = 0;

    public void addLatency(long latency) {
        cumulated += latency;
        nMessages++;
    }

    public long getAverage() {
        if (nMessages == 0)
            return 0;
        return Math.round((double) cumulated / (double) nMessages);
    }

    public void reset() {
        cumulated = 0;
        nMessages = 0;
    }
}
