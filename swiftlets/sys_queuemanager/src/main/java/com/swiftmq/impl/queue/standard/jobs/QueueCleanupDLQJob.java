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

package com.swiftmq.impl.queue.standard.jobs;

import com.swiftmq.impl.queue.standard.QueueManagerImpl;
import com.swiftmq.impl.queue.standard.SwiftletContext;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.swiftlet.scheduler.Job;
import com.swiftmq.swiftlet.scheduler.JobException;
import com.swiftmq.swiftlet.scheduler.JobTerminationListener;
import com.swiftmq.tools.sql.LikeComparator;

import java.util.Properties;

public class QueueCleanupDLQJob implements Job
{
  SwiftletContext ctx = null;
  boolean stopCalled = false;
  Properties properties = null;
  QueueReceiver receiver = null;
  QueuePullTransaction pullTx = null;
  QueueSender sender = null;
  QueuePushTransaction pushTx = null;
  QueueImpl targetQueue = null;

  public QueueCleanupDLQJob(SwiftletContext ctx)
  {
    this.ctx = ctx;
  }

  private void closeResources()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/closeResources");
    try
    {
      if (pullTx != null)
        pullTx.rollback();
      pullTx = null;
    } catch (Exception e)
    {
    }
    try
    {
      if (receiver != null)
        receiver.close();
      receiver = null;
    } catch (Exception e)
    {
    }
    try
    {
      if (pushTx != null)
        pushTx.rollback();
      pushTx = null;
    } catch (Exception e)
    {
    }
    try
    {
      if (sender != null)
        sender.close();
      sender = null;
    } catch (Exception e)
    {
    }
  }

  private int cleanupQueue(String queueName) throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/cleanupQueue ...");
    receiver = ctx.queueManager.createQueueReceiver(queueName, null, null);
    pullTx = receiver.createTransaction(false);
    targetQueue = new QueueImpl(QueueManagerImpl.DLQ + '@' + SwiftletManager.getInstance().getRouterName());
    sender = ctx.queueManager.createQueueSender(targetQueue.getQueueName(), null);
    pushTx = sender.createTransaction();
    if (stopCalled)
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/cleanupQueue, stopCalled (2)");
      closeResources();
      return 0;
    }
    int cnt = 0;
    MessageEntry entry = null;
    while ((entry = pullTx.getExpiredMessage(0)) != null)
    {
      MessageImpl msg = entry.getMessage();
      pushTx.putMessage(msg);
      cnt++;
      pushTx.commit();
      pullTx.commit();
      if (stopCalled)
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/cleanupQueue, stopCalled (2)");
        closeResources();
        return cnt;
      }
      pullTx = receiver.createTransaction(false);
      pushTx = sender.createTransaction();
    }
    closeResources();
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/cleanupQueue done, cnt=" + cnt);
    return cnt;
  }

  public void start(Properties properties, JobTerminationListener jobTerminationListener) throws JobException
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/start, properties=" + properties + " ...");
    this.properties = properties;
    StringBuffer termMsg = new StringBuffer();

    try
    {
      String predicate = properties.getProperty("Queue Name Predicate");
      String[] names = ctx.queueManager.getDefinedQueueNames();
      if (names != null)
      {
        for (int i = 0; i < names.length; i++)
        {
          if (!names[i].startsWith("tpc$"))
          {
            if (LikeComparator.compare(names[i], predicate, '\\'))
            {
              if (termMsg.length() > 0)
                termMsg.append(", ");
              termMsg.append(names[i]);
              termMsg.append("=");
              try
              {
                termMsg.append(cleanupQueue(names[i]));
              } catch (Exception e)
              {
                if (ctx.traceSpace.enabled)
                  ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/exception=" + e);
                termMsg.append(e.toString());
              }
            }
          }
          if (stopCalled)
            break;
        }
      }
    } catch (Exception e)
    {
      closeResources();
      throw new JobException(e.toString(), e, false);
    }
    closeResources();
    jobTerminationListener.jobTerminated(termMsg.toString());
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/start, properties=" + properties + " done, " + termMsg);
  }

  public void stop() throws JobException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/stop ...");
    stopCalled = true;
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/stop done");
  }

  public String toString()
  {
    return "[QueueCleanupDLQJob, properties=" + properties + "]";
  }
}
