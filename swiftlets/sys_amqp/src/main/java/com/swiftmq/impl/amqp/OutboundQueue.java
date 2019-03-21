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

package com.swiftmq.impl.amqp;

import com.swiftmq.amqp.Writable;
import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.tools.queue.SingleProcessorQueue;
import com.swiftmq.tools.util.DataStreamOutputStream;

public class OutboundQueue extends SingleProcessorQueue
{
  SwiftletContext ctx;
  ThreadPool pool;
  VersionedConnection connection;
  QueueProcessor queueProcessor;
  DataStreamOutputStream dos = null;
  OutboundTracer outboundTracer = null;

  OutboundQueue(SwiftletContext ctx, ThreadPool pool, VersionedConnection connection)
  {
    super(100);
    this.ctx = ctx;
    this.pool = pool;
    this.connection = connection;
    dos = new DataStreamOutputStream(connection.getConnection().getOutputStream());
    queueProcessor = new QueueProcessor();
  }

  public void setOutboundTracer(OutboundTracer outboundTracer)
  {
    this.outboundTracer = outboundTracer;
  }

  protected void startProcessor()
  {
    if (!connection.closed)
      pool.dispatchTask(queueProcessor);
  }

  protected void process(Object[] bulk, int n)
  {
    try
    {
      for (int i = 0; i < n; i++)
      {
        ((Writable) bulk[i]).writeContent(dos);
        if (outboundTracer != null && ctx.protSpace.enabled)
          ctx.protSpace.trace(outboundTracer.getTraceKey(), connection.toString() + "/SND: " + outboundTracer.getTraceString(bulk[i]));
      }
      dos.flush();
    } catch (Exception e)
    {
      connection.close();
    } finally
    {
      for (int i = 0; i < n; i++)
      {
        Writable w = (Writable) bulk[i];
        if (w.getSemaphore() != null)
          w.getSemaphore().notifySingleWaiter();
        else if (w.getCallback() != null)
          w.getCallback().done(true);
      }
    }
  }

  private class QueueProcessor implements AsyncTask
  {
    public boolean isValid()
    {
      return !connection.closed;
    }

    public String getDispatchToken()
    {
      return VersionedConnection.TP_CONNECTIONSVC;
    }

    public String getDescription()
    {
      return connection.toString() + "/QueueProcessor";
    }

    public void stop()
    {
    }

    public void run()
    {
      if (!connection.closed && dequeue())
        pool.dispatchTask(this);
    }
  }
}
