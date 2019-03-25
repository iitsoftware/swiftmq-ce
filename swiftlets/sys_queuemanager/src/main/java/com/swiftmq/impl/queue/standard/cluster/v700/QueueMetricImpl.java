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

import com.swiftmq.impl.queue.standard.cluster.QueueMetric;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class QueueMetricImpl implements QueueMetric {
    String queueName = null;
    boolean hasReceiver = false;
    String routerName = null;
    boolean redispatch = false;

    public QueueMetricImpl(String queueName, boolean hasReceiver, boolean redispatch) {
        this.queueName = queueName;
        this.hasReceiver = hasReceiver;
        this.redispatch = redispatch;
    }

    public QueueMetricImpl() {
    }

    public String getQueueName() {
        return queueName;
    }

    public void setHasReceiver(boolean b) {
        hasReceiver = b;
    }

    public boolean hasReceiver() {
        return hasReceiver;
    }

    public String getRouterName() {
        return routerName;
    }

    public void setRouterName(String routerName) {
        this.routerName = routerName;
    }

    public void setRedispatch(boolean redispatch) {
        this.redispatch = redispatch;
    }

    public boolean isRedispatch() {
        return redispatch;
    }

    public int getDumpId() {
        return MetricFactory.QUEUE_METRIC;
    }

    public void writeContent(DataOutput out) throws IOException {
        out.writeUTF(queueName);
        out.writeBoolean(hasReceiver);
    }

    public void readContent(DataInput in) throws IOException {
        queueName = in.readUTF();
        hasReceiver = in.readBoolean();
    }

    public String toString() {
        return "[QueueMetricImpl (v700), queueName=" + queueName + ", hasReceiver=" + hasReceiver + "]";
    }
}
