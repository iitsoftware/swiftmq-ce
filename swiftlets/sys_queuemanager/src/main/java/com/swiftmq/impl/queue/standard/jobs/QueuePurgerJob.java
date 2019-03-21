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

import com.swiftmq.impl.queue.standard.SwiftletContext;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.queue.QueuePullTransaction;
import com.swiftmq.swiftlet.queue.QueueReceiver;
import com.swiftmq.swiftlet.scheduler.Job;
import com.swiftmq.swiftlet.scheduler.JobException;
import com.swiftmq.swiftlet.scheduler.JobTerminationListener;

import javax.jms.InvalidSelectorException;
import java.util.Properties;

public class QueuePurgerJob implements Job
{
  SwiftletContext ctx = null;
  boolean stopCalled = false;
  Properties properties = null;
  QueueReceiver receiver = null;
  QueuePullTransaction transaction = null;

  public QueuePurgerJob(SwiftletContext ctx)
  {
    this.ctx = ctx;
  }

  private void terminate()
  {
    try
    {
      if (transaction != null)
        transaction.rollback();
      transaction = null;
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
  }

  public void start(Properties properties, JobTerminationListener jobTerminationListener) throws JobException
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/start, properties=" + properties + " ...");
    this.properties = properties;
    int cnt = 0;
    String queueName = properties.getProperty("Queue Name");
    if (!queueName.startsWith("tpc$"))
    {
      MessageSelector selector = null;
      String s = properties.getProperty("Message Selector");
      if (s != null)
      {
        selector = new MessageSelector(s);
        try
        {
          selector.compile();
        } catch (InvalidSelectorException e)
        {
          throw new JobException(e.toString(), e, false);
        }
      }
      try
      {
        receiver = ctx.queueManager.createQueueReceiver(queueName, null, selector);
        transaction = receiver.createTransaction(false);
      } catch (Exception e)
      {
        throw new JobException(e.toString(), e, false);
      }
      if (stopCalled)
      {
        terminate();
        return;
      }
      try
      {
        while (transaction.getMessage(0, selector) != null)
        {
          cnt++;
          transaction.commit();
          if (stopCalled)
          {
            terminate();
            return;
          }
          transaction = receiver.createTransaction(false);
        }
      } catch (Exception e)
      {
        terminate();
        throw new JobException(e.toString(), e, false);
      }
    }
    terminate();
    jobTerminationListener.jobTerminated(cnt + " Messages purged");
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/start, properties=" + properties + " done, cnt=" + cnt);
  }

  public void stop() throws JobException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/stop ...");
    stopCalled = true;
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/stop done");
  }

  public String toString()
  {
    return "[QueuePurgerJob, properties=" + properties + "]";
  }
}
