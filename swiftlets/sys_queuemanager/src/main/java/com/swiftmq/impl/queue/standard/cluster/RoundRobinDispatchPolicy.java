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

import com.swiftmq.impl.queue.standard.SwiftletContext;
import com.swiftmq.impl.queue.standard.cluster.v700.ClusteredQueueMetricImpl;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.swiftlet.queue.AbstractQueue;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class RoundRobinDispatchPolicy implements DispatchPolicy
{
  SwiftletContext ctx = null;
  String clusteredQueueName = null;
  List localMetrics = new LinkedList();
  int nextLocal = -1;
  List receiverMetrics = new LinkedList();
  int nextReceiver = -1;
  List noReceiverMetrics = new LinkedList();
  int nextNoReceiver = -1;
  DispatchPolicyListener listener = null;

  public RoundRobinDispatchPolicy(SwiftletContext ctx, String clusteredQueueName)
  {
    this.ctx = ctx;
    this.clusteredQueueName = clusteredQueueName;
  }

  private void removeRouterMetrics(String routerName, List list)
  {
    for (Iterator iter = list.iterator(); iter.hasNext();)
    {
      QueueMetric qm = (QueueMetric) iter.next();
      if (qm.getRouterName() != null && qm.getRouterName().equals(routerName))
      {
        iter.remove();
        if (listener != null)
          listener.dispatchQueueRemoved(qm.getQueueName());
      }
    }
  }

  private QueueMetric getMetric(String queueName, List list)
  {
    for (Iterator iter = list.iterator(); iter.hasNext();)
    {
      QueueMetric qm = (QueueMetric) iter.next();
      if (qm.getQueueName().equals(queueName))
        return qm;
    }
    return null;
  }

  public synchronized void setDispatchPolicyListener(DispatchPolicyListener listener)
  {
    this.listener = listener;
  }

  public synchronized void receiverCountChanged(AbstractQueue abstractQueue, int receiverCount)
  {
    if (receiverCount == 0 || receiverCount == 1)
    {
      QueueMetric qm = getMetric(abstractQueue.getQueueName(), localMetrics);
      if (qm != null)
      {
        if (qm.hasReceiver())
          receiverMetrics.remove(qm);
        else
          noReceiverMetrics.remove(qm);
        qm.setHasReceiver(receiverCount == 1);
        if (qm.hasReceiver())
          receiverMetrics.add(qm);
        else
          noReceiverMetrics.add(qm);
      }
    }
  }

  public synchronized void addLocalMetric(QueueMetric metric)
  {
    localMetrics.add(metric);
    if (metric.hasReceiver())
      receiverMetrics.add(metric);
    else
      noReceiverMetrics.add(metric);
  }

  public synchronized void removeLocalMetric(QueueMetric metric)
  {
    localMetrics.remove(metric);
    if (metric.hasReceiver())
      receiverMetrics.remove(metric);
    else
      noReceiverMetrics.remove(metric);
    if (listener != null)
      listener.dispatchQueueRemoved(metric.getQueueName());
  }

  public synchronized ClusteredQueueMetric getLocalMetric()
  {
    int rcount = receiverMetrics.size();
    List cloned = (List) ((LinkedList) localMetrics).clone();
    for (int i = 0; i < cloned.size(); i++)
    {
      QueueMetric qm = (QueueMetric) cloned.get(i);
      if (qm.hasReceiver())
        rcount++;
    }
    return new ClusteredQueueMetricImpl(clusteredQueueName, cloned, rcount > 0);
  }

  public synchronized void addMetric(String routerName, ClusteredQueueMetric metric)
  {
    removeRouterMetrics(routerName, receiverMetrics);
    removeRouterMetrics(routerName, noReceiverMetrics);
    List list = metric.getQueueMetrics();
    if (list != null)
    {
      for (int i = 0; i < list.size(); i++)
      {
        QueueMetric qm = (QueueMetric) list.get(i);
        qm.setRouterName(routerName);
        if (qm.hasReceiver())
          receiverMetrics.add(qm);
        else
          noReceiverMetrics.add(qm);
      }
    }
  }

  public synchronized void removeMetric(String routerName)
  {
    removeRouterMetrics(routerName, receiverMetrics);
    removeRouterMetrics(routerName, noReceiverMetrics);
  }

  public synchronized boolean isReceiverSomewhere()
  {
    return receiverMetrics.size() > 0;
  }

  public boolean isMessageBasedDispatch()
  {
    return false;
  }

  public synchronized String getNextSendQueue()
  {
    if (receiverMetrics.size() > 0)
    {
      nextReceiver++;
      if (nextReceiver > receiverMetrics.size() - 1)
        nextReceiver = 0;
      return ((QueueMetric) receiverMetrics.get(nextReceiver)).getQueueName();
    }
    if (noReceiverMetrics.size() > 0)
    {
      nextNoReceiver++;
      if (nextNoReceiver > noReceiverMetrics.size() - 1)
        nextNoReceiver = 0;
      return ((QueueMetric) noReceiverMetrics.get(nextNoReceiver)).getQueueName();
    }
    return null;
  }

  public String getNextSendQueue(MessageImpl message)
  {
    return getNextSendQueue();
  }

  public synchronized String getNextReceiveQueue()
  {
    if (localMetrics.size() == 0)
      return null;
    nextLocal++;
    if (nextLocal > localMetrics.size() - 1)
      nextLocal = 0;
    return ((QueueMetric) localMetrics.get(nextLocal)).getQueueName();
  }

  public void close()
  {
    localMetrics.clear();
    receiverMetrics.clear();
    noReceiverMetrics.clear();
    nextLocal = -1;
    nextReceiver = -1;
    nextNoReceiver = -1;
  }
}
