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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RedispatcherController
{
  SwiftletContext ctx = null;
  Map redispatchers = new HashMap();

  public RedispatcherController(SwiftletContext ctx)
  {
    this.ctx = ctx;
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/stop");
  }

  public synchronized void redispatch(String sourceQueueName, String targetQueueName)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/redispatch, source=" + sourceQueueName + ", target=" + targetQueueName + " ...");
    if (redispatchers.get(sourceQueueName) != null)
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/redispatch, source=" + sourceQueueName + ", target=" + targetQueueName + ", already running, do nothing!");
      return;
    }
    try
    {
      Redispatcher rdp = new Redispatcher(ctx, sourceQueueName, targetQueueName);
      redispatchers.put(sourceQueueName, rdp);
      rdp.start();
    } catch (Exception e)
    {
      e.printStackTrace();
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/redispatch, source=" + sourceQueueName + ", target=" + targetQueueName + ", exception=" + e);
    }
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/redispatch, source=" + sourceQueueName + ", target=" + targetQueueName + " done");
  }

  public synchronized void redispatcherFinished(String sourceQueueName)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/redispatcherFinished, source=" + sourceQueueName);
    redispatchers.remove(sourceQueueName);
  }

  public void close()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/close ...");
    Map cloned = null;
    synchronized (this)
    {
      cloned = (Map) ((HashMap) redispatchers).clone();
      redispatchers.clear();
    }
    for (Iterator iter = cloned.entrySet().iterator(); iter.hasNext();)
    {
      ((Redispatcher) ((Map.Entry) iter.next()).getValue()).stop();
    }
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/close done");
  }

  public String toString()
  {
    return "RedispatcherController";
  }
}
