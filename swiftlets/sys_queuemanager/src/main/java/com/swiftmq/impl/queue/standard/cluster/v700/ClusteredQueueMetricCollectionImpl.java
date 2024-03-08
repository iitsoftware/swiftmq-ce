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

import com.swiftmq.impl.queue.standard.cluster.ClusteredQueueMetricCollection;
import com.swiftmq.tools.dump.Dumpalizer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ClusteredQueueMetricCollectionImpl implements ClusteredQueueMetricCollection {
    String routerName = null;
    List clusteredQueueMetrics = null;

    public ClusteredQueueMetricCollectionImpl(String routerName, List clusteredQueueMetrics) {
        this.routerName = routerName;
        this.clusteredQueueMetrics = clusteredQueueMetrics;
    }

    public ClusteredQueueMetricCollectionImpl() {
    }

    public String getRouterName() {
        return routerName;
    }

    public List getClusteredQueueMetrics() {
        return clusteredQueueMetrics;
    }

    public int getDumpId() {
        return MetricFactory.CLUSTERED_QUEUE_METRIC_COLLECTION;
    }

    public void writeContent(DataOutput out) throws IOException {
        out.writeUTF(routerName);
        if (clusteredQueueMetrics != null) {
            out.writeBoolean(true);
            out.writeInt(clusteredQueueMetrics.size());
            for (Object clusteredQueueMetric : clusteredQueueMetrics)
                Dumpalizer.dump(out, (ClusteredQueueMetricImpl) clusteredQueueMetric);
        } else
            out.writeBoolean(false);
    }

    public void readContent(DataInput in) throws IOException {
        routerName = in.readUTF();
        if (in.readBoolean()) {
            int size = in.readInt();
            clusteredQueueMetrics = new ArrayList(size);
            for (int i = 0; i < size; i++)
                clusteredQueueMetrics.add(Dumpalizer.construct(in, MetricFactory.FACTORY));
        }
    }
}
