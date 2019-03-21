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
import com.swiftmq.swiftlet.scheduler.JobFactory;
import com.swiftmq.swiftlet.scheduler.JobParameter;

import java.util.HashMap;
import java.util.Map;

public class QueueMoverJobFactory implements JobFactory
{
  SwiftletContext ctx = null;
  Map parameters = new HashMap();

  public QueueMoverJobFactory(SwiftletContext ctx)
  {
    this.ctx = ctx;
    JobParameter p = new JobParameter("Source Queue","Source Queue Name",null,true,new QueueNameVerifier(ctx));
    parameters.put(p.getName(),p);
    p = new JobParameter("Target Queue","Target Queue Name",null,true,null);
    parameters.put(p.getName(),p);
    p = new JobParameter("Message Selector","Message Selector to use (optional)",null,false,new SelectorVerifier());
    parameters.put(p.getName(),p);
  }

  public String getName()
  {
    return "Queue Mover";
  }

  public String getDescription()
  {
    return "Moves the Content of a Source Queue to a Target Queue";
  }

  public Map getJobParameters()
  {
    return parameters;
  }

  public JobParameter getJobParameter(String s)
  {
    return (JobParameter)parameters.get(s);
  }

  public Job getJobInstance()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(),toString()+"/getJobInstance");
    return new QueueMoverJob(ctx);
  }

  public void finished(Job job, JobException e)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(),toString()+"/finished, job="+job+", jobException="+e);
  }

  public String toString()
  {
    return "[QueueMoverJobFactory]";
  }
}
