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

package com.swiftmq.impl.jms.standard.v500;

import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.tools.queue.SingleProcessorQueue;
import com.swiftmq.tools.requestreply.Request;

public class SessionQueue extends SingleProcessorQueue
{
  ThreadPool pool;
  Session session;
  QueueProcessor queueProcessor;

  SessionQueue(ThreadPool pool, Session session)
  {
    super(100);
    this.pool = pool;
    this.session = session;
    queueProcessor = new QueueProcessor();
  }

  protected void startProcessor()
  {
    if (!session.closed)
      pool.dispatchTask(queueProcessor);
  }

  /**
   * @param obj
   */
  protected void process(Object[] bulk, int n)
  {
    for (int i=0;i<n;i++)
    {
      if (!session.closed)
        ((Request)bulk[i]).accept(session);
      else
        break;
    }
  }

  private class QueueProcessor implements AsyncTask
  {
    public boolean isValid()
    {
      return !session.closed;
    }

    public String getDispatchToken()
    {
      return Session.TP_SESSIONSVC;
    }

    public String getDescription()
    {
      return session.toString() + "/QueueProcessor";
    }

    public void stop()
    {
    }

    public void run()
    {
      if (!session.closed && dequeue())
        pool.dispatchTask(this);
    }
  }
}

