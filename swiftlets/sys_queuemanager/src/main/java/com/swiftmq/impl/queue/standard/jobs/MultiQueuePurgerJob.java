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
import com.swiftmq.swiftlet.scheduler.Job;
import com.swiftmq.swiftlet.scheduler.JobException;
import com.swiftmq.swiftlet.scheduler.JobTerminationListener;
import com.swiftmq.tools.sql.LikeComparator;

import java.util.Properties;

public class MultiQueuePurgerJob implements Job, JobTerminationListener
{
  SwiftletContext ctx = null;
  volatile boolean stopCalled = false;
  Properties properties = null;
  volatile QueuePurgerJob currentJob = null;
  StringBuffer termMsg = null;

  public MultiQueuePurgerJob(SwiftletContext ctx)
  {
    this.ctx = ctx;
  }

  private void terminate()
  {
    try
    {
      if (currentJob != null)
        currentJob.stop();
    } catch (JobException e)
    {
    }
  }

  public void jobTerminated()
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/jobTerminated, currentJob=" + currentJob);
  }

  public void jobTerminated(String s)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/jobTerminated, currentJob=" + currentJob + ", message=" + s);
    termMsg.append(s);
  }

  public void jobTerminated(JobException e)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/jobTerminated, currentJob=" + currentJob + ", exception=" + e);
  }

  public void start(Properties properties, JobTerminationListener jobTerminationListener) throws JobException
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/start, properties=" + properties + " ...");
    this.properties = properties;
    termMsg = new StringBuffer();
    if (stopCalled)
    {
      terminate();
      return;
    }
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
              properties.put("Queue Name", names[i]);
              currentJob = new QueuePurgerJob(ctx);
              try
              {
                currentJob.start(properties, this);
              } catch (JobException e)
              {
                termMsg.append(e.toString());
              }
            }
            if (stopCalled)
              break;
          }
        }
      }
    } catch (Exception e)
    {
      terminate();
      throw new JobException(e.toString(), e, false);
    }
    terminate();
    jobTerminationListener.jobTerminated(termMsg.toString());
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/finished, properties=" + properties);
  }

  public void stop() throws JobException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/stop ...");
    stopCalled = true;
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/stop done");
  }

  public String toString()
  {
    return "[MultiQueuePurgerJob, properties=" + properties + "]";
  }
}
