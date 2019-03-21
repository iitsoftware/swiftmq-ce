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

import com.swiftmq.impl.queue.standard.cluster.ClusteredQueueMetric;
import com.swiftmq.tools.dump.Dumpalizer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class ClusteredQueueMetricImpl implements ClusteredQueueMetric
{
  String clusteredQueueName = null;
  List queueMetrics = null;
  boolean receiverSomewhere = false;

  public ClusteredQueueMetricImpl(String clusteredQueueName, List queueMetrics, boolean receiverSomewhere)
  {
    this.clusteredQueueName = clusteredQueueName;
    this.queueMetrics = queueMetrics;
    this.receiverSomewhere = receiverSomewhere;
  }

  public ClusteredQueueMetricImpl()
  {
  }

  public String getClusteredQueueName()
  {
    return clusteredQueueName;
  }

  public List getQueueMetrics()
  {
    return queueMetrics;
  }

  public boolean isReceiverSomewhere()
  {
    return receiverSomewhere;
  }

  public void setReceiverSomewhere(boolean b)
  {
    receiverSomewhere = b;
  }

  public int getDumpId()
  {
    return MetricFactory.CLUSTERED_QUEUE_METRIC;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    out.writeUTF(clusteredQueueName);
    if (queueMetrics != null)
    {
      out.writeBoolean(true);
      out.writeInt(queueMetrics.size());
      for (int i = 0; i < queueMetrics.size(); i++)
        Dumpalizer.dump(out, (QueueMetricImpl) queueMetrics.get(i));
    } else
      out.writeBoolean(false);
  }

  public void readContent(DataInput in) throws IOException
  {
    clusteredQueueName = in.readUTF();
    if (in.readBoolean())
    {
      int size = in.readInt();
      queueMetrics = new LinkedList();
      for (int i = 0; i < size; i++)
        queueMetrics.add(Dumpalizer.construct(in, MetricFactory.FACTORY));
    }
  }

  public String toString()
  {
    return "[ClusteredQueueMetricImpl (v700), clusteredQueueName=" + clusteredQueueName + ", queueMetrics=" + queueMetrics + "]";
  }
}
