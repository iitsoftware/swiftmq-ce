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

package com.swiftmq.impl.queue.standard.cluster.v700;

import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.DumpableFactory;

public class MetricFactory extends DumpableFactory {
    public static final MetricFactory FACTORY = new MetricFactory();
    public static final int CLUSTERED_QUEUE_METRIC_COLLECTION = 0;
    public static final int CLUSTERED_QUEUE_METRIC = 1;
    public static final int QUEUE_METRIC = 2;

    public Dumpable createDumpable(int dumpId) {
        Dumpable d = null;
        switch (dumpId) {
            case CLUSTERED_QUEUE_METRIC_COLLECTION:
                d = new ClusteredQueueMetricCollectionImpl();
                break;
            case CLUSTERED_QUEUE_METRIC:
                d = new ClusteredQueueMetricImpl();
                break;
            case QUEUE_METRIC:
                d = new QueueMetricImpl();
                break;
            default:
                throw new NullPointerException("Can't create dumpable, invalid dumpid (" + dumpId + ")");
        }
        return d;
    }
}
