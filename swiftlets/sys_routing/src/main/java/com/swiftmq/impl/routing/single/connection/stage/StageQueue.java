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

package com.swiftmq.impl.routing.single.connection.stage;

import com.swiftmq.swiftlet.threadpool.*;
import com.swiftmq.tools.queue.SingleProcessorQueue;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.impl.routing.single.SwiftletContext;
import com.swiftmq.impl.routing.single.smqpr.*;

public class StageQueue extends SingleProcessorQueue
{
  static final String TP_SERVICE = "sys$routing.connection.service";

  SwiftletContext ctx = null;
  ThreadPool myTP = null;
  boolean closed = false;
  QueueProcessor queueProcessor = null;
  Stage stage = null;

  public StageQueue(SwiftletContext ctx)
  {
    this.ctx = ctx;
    myTP = ctx.threadpoolSwiftlet.getPool(TP_SERVICE);
    queueProcessor = new QueueProcessor();
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "StageQueue created");
  }

  public void closePreviousStage()
  {
    if (stage != null)
      stage.close();
  }

  public void setStage(Stage stage)
  {
    if (this.stage != null)
      closePreviousStage();
    this.stage = stage;
    if (stage != null)
    {
      stage.setStageQueue(this);
      stage.init();
    }
  }

  public Stage getStage()
  {
    return stage;
  }

  protected void startProcessor()
  {
    myTP.dispatchTask(queueProcessor);
  }

  protected void process(Object[] bulk, int n)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "StageQueue/process, n=" + n);
    for (int i = 0; i < n; i++)
    {
      Request r = (Request)bulk[i];
      if (r.getDumpId() == SMQRFactory.CLOSE_STAGE_QUEUE_REQ)
      {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "StageQueue/receiving: "+r);
        close();
        Semaphore sem = ((CloseStageQueueRequest)r).getSemaphore();
        if (sem != null)
          sem.notifySingleWaiter();
        return;
      }
      if (stage != null)
        stage.process((Request)bulk[i]);
    }
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "StageQueue/process, done");
  }

  public synchronized void close()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "StageQueue/close...");
    super.close();
    if (stage != null)
      stage.close();
    closed = true;
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "StageQueue/close done");
  }

  private class QueueProcessor implements AsyncTask
  {

    public boolean isValid()
    {
      return !closed;
    }

    public String getDispatchToken()
    {
      return TP_SERVICE;
    }

    public String getDescription()
    {
      return ctx.routingSwiftlet.getName() + "/StageQueue/QueueProcessor";
    }

    public void stop()
    {
    }

    public void run()
    {
      if (dequeue() && !closed)
        myTP.dispatchTask(this);
    }
  }
}
