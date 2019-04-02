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

package jms.base;

import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.naming.InitialContext;

public class PTPTestCase extends JMSTestCase
{
  public InitialContext ctx = null;
  QueueConnectionFactory qcf = null;

  public PTPTestCase(String name)
  {
    super(name);
  }

  public QueueConnection createQueueConnection(InitialContext ctx, String lookup, boolean start)
  {
    QueueConnection qc = null;
    try
    {
      if (qcf == null)
        qcf = (QueueConnectionFactory) ctx.lookup(lookup);
      qc = qcf.createQueueConnection();
      if (start)
        qc.start();
    } catch (Exception e)
    {
      failFast("create queue connection failed: " + e);
    }
    return qc;
  }

  public QueueConnection createQueueConnection(InitialContext ctx, String lookup)
  {
    QueueConnection qc = null;
    try
    {
      if (ctx == null)
        ctx = createInitialContext();
      qc = createQueueConnection(ctx, lookup, false);
    } catch (Exception e)
    {
      failFast("create queue connection failed: " + e);
    }
    return qc;
  }

  public QueueConnection createQueueConnection(String lookup)
  {
    QueueConnection qc = null;
    try
    {
      if (ctx == null)
        ctx = createInitialContext();
      qc = createQueueConnection(ctx, lookup);
    } catch (Exception e)
    {
      failFast("create queue connection failed: " + e);
    }
    return qc;
  }

  public QueueConnection createQueueConnection(String lookup, boolean start)
  {
    QueueConnection qc = null;
    try
    {
      if (ctx == null)
        ctx = createInitialContext();
      qc = createQueueConnection(ctx, lookup, start);
    } catch (Exception e)
    {
      failFast("create queue connection failed: " + e);
    }
    return qc;
  }

  public Queue getQueue(String name)
  {
    Queue queue = null;
    try
    {
      if (ctx == null)
        ctx = createInitialContext();
      queue = (Queue) ctx.lookup(name);
    } catch (Exception e)
    {
      failFast("get queue failed: " + e);
    }
    return queue;
  }

  protected void tearDown() throws Exception
  {
    if (ctx != null)
      ctx.close();
    ctx = null;
    qcf = null;
    super.tearDown();
  }
}

