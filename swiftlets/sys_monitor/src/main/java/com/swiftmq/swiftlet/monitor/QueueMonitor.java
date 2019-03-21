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

import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.queue.QueueManager;
import com.swiftmq.swiftlet.timer.event.TimerListener;

import java.util.HashMap;
import java.util.Map;

public class QueueMonitor implements TimerListener
{
  SwiftletContext ctx = null;
  QueueManager queueManager = null;
  Property queueSizeStartProp = null;
  Map<String, Integer> criticals = new HashMap<String, Integer>();

  public QueueMonitor(SwiftletContext ctx)
  {
    this.ctx = ctx;
    queueSizeStartProp = ctx.root.getEntity("queue").getProperty("queue-max-message-threshold");
    queueManager = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/created");
  }

  public void performTimeAction()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/performTimeAction");
    String[] names = queueManager.getDefinedQueueNames();
    if (names != null)
    {
      for (int i = 0; i < names.length; i++)
      {
        if (!names[i].startsWith("tpc$"))
        {
          AbstractQueue queue = queueManager.getQueueForInternalUse(names[i]);
          if (queue != null)
          {
            try
            {
              int threshold = queue.getMonitorAlertThreshold() > 0 ? queue.getMonitorAlertThreshold() : ((Integer) queueSizeStartProp.getValue()).intValue();
              if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/performTimeAction, Queue " + queue.getQueueName() + " size: " + queue.getNumberQueueMessages() + ", threshold: " + threshold);
              if (threshold == -1)
                continue;
              if (queue.getNumberQueueMessages() > threshold)
              {
                if (criticals.get(queue.getQueueName()) == null)
                {
                  criticals.put(queue.getQueueName(), (int) queue.getNumberQueueMessages());
                  ctx.mailGenerator.generateMail("Queue Monitor on " + SwiftletManager.getInstance().getRouterName() + ": Queue " + queue.getQueueName() + " Size CRITICAL!",
                      "Queue " + queue.getQueueName() + " has currently " + queue.getNumberQueueMessages() + " messages in backlog, alert threshold is " + threshold + " messages!\r\n");
                  if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/performTimeAction, Queue Size CRITICAL! Queue " + queue.getQueueName() + " has currently " + queue.getNumberQueueMessages() + " messages in backlog, alert threshold is " + threshold + " messages!");
                  ctx.logSwiftlet.logWarning(ctx.swiftlet.getName(), toString() + "/Queue Size CRITICAL! Queue " + queue.getQueueName() + " has currently " + queue.getNumberQueueMessages() + " messages in backlog, alert threshold is " + threshold + " messages!");
                }
              } else
              {
                if (criticals.get(queue.getQueueName()) != null)
                {
                  criticals.remove(queue.getQueueName());
                  ctx.mailGenerator.generateMail("Queue Monitor on " + SwiftletManager.getInstance().getRouterName() + ": Queue " + queue.getQueueName() + " Size NORMALIZED!",
                      "Queue " + queue.getQueueName() + " has currently " + queue.getNumberQueueMessages() + " messages in backlog, alert threshold is " + threshold + " messages!\r\n");
                  if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/performTimeAction, Queue Size NORMALIZED! Queue " + queue.getQueueName() + " has currently " + queue.getNumberQueueMessages() + " messages in backlog, alert threshold is " + threshold + " messages!");
                  ctx.logSwiftlet.logWarning(ctx.swiftlet.getName(), toString() + "/Queue Size NORMALIZED! Queue " + queue.getQueueName() + " has currently " + queue.getNumberQueueMessages() + " messages in backlog, alert threshold is " + threshold + " messages!");
                }
              }
            } catch (QueueException e)
            {
            }
          }
        }
      }
    }
  }

  public void close()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/close");
  }

  public String toString()
  {
    return "QueueMonitor";
  }
}
