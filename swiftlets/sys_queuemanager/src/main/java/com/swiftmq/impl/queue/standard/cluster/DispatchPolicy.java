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

package com.swiftmq.impl.queue.standard.cluster;

import com.swiftmq.jms.MessageImpl;
import com.swiftmq.swiftlet.queue.event.QueueReceiverListener;

public interface DispatchPolicy extends QueueReceiverListener {
    void setDispatchPolicyListener(DispatchPolicyListener l);

    void addLocalMetric(QueueMetric metric);

    void removeLocalMetric(QueueMetric metric);

    ClusteredQueueMetric getLocalMetric();

    void addMetric(String routerName, ClusteredQueueMetric metric);

    void removeMetric(String routerName);

    boolean isReceiverSomewhere();

    boolean isMessageBasedDispatch();

    String getNextSendQueue();

    String getNextSendQueue(MessageImpl message);

    String getNextReceiveQueue();

    void close();
}
