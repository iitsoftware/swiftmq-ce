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
import javax.jms.XAQueueConnection;
import javax.jms.XAQueueConnectionFactory;
import javax.naming.InitialContext;

public class XAPTPTestCase extends JMSTestCase
{
  public InitialContext ctx = null;
  XAQueueConnectionFactory qcf = null;

  public XAPTPTestCase(String name)
  {
    super(name);
  }

  public XAQueueConnection createXAQueueConnection(InitialContext ctx, String lookup)
  {
    return createXAQueueConnection(ctx, lookup, true);
  }

  public XAQueueConnection createXAQueueConnection(InitialContext ctx, String lookup, boolean start)
  {
    XAQueueConnection qc = null;
    try
    {
      if (qcf == null)
        qcf = (XAQueueConnectionFactory) ctx.lookup(lookup);
      qc = qcf.createXAQueueConnection();
      if (start)
        qc.start();
    } catch (Exception e)
    {
      failFast("create xa queue connection failed: " + e);
    }
    return qc;
  }

  public XAQueueConnection createXAQueueConnection(String lookup)
  {

    return createXAQueueConnection(lookup, true);
  }

  public XAQueueConnection createXAQueueConnection(String lookup, boolean start)
  {
    XAQueueConnection qc = null;
    try
    {
      if (ctx == null)
        ctx = createInitialContext();
      qc = createXAQueueConnection(ctx, lookup, start);
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
