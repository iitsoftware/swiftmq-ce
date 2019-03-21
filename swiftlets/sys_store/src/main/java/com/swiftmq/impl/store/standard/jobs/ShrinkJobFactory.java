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

package com.swiftmq.impl.store.standard.jobs;

import com.swiftmq.swiftlet.scheduler.*;
import com.swiftmq.impl.store.standard.StoreContext;

import java.util.*;

public class ShrinkJobFactory implements JobFactory
{
  StoreContext ctx = null;
  Map parameters = new HashMap();

  public ShrinkJobFactory(StoreContext ctx)
  {
    this.ctx = ctx;
  }

  public String getName()
  {
    return "Shrink";
  }

  public String getDescription()
  {
    return "Performs a Shrink of the page.db";
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
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(),toString()+"/getJobInstance");
    return new ShrinkJob(ctx);
  }

  public void finished(Job job, JobException e)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(),toString()+"/finished, job="+job+", jobException="+e);
  }

  public String toString()
  {
    return "[ShrinkJobFactory]";
  }
}