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
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.naming.InitialContext;

public class UnifiedXAPTPTestCase extends JMSTestCase
{
  public InitialContext ctx = null;
  XAConnectionFactory qcf = null;

  public UnifiedXAPTPTestCase(String name)
  {
    super(name);
  }

  public XAConnection createXAConnection(InitialContext ctx, String lookup)
  {
    return createXAConnection(ctx, lookup, true);
  }

  public XAConnection createXAConnection(InitialContext ctx, String lookup, boolean start)
  {
    XAConnection qc = null;
    try
    {
      if (qcf == null)
        qcf = (XAConnectionFactory) ctx.lookup(lookup);
      qc = qcf.createXAConnection();
      if (start)
        qc.start();
    } catch (Exception e)
    {
      fail("create xa queue connection failed: " + e);
    }
    return qc;
  }

  public XAConnection createXAConnection(String lookup)
  {

    return createXAConnection(lookup, true);
  }

  public XAConnection createXAConnection(String lookup, boolean start)
  {
    XAConnection qc = null;
    try
    {
      if (ctx == null)
        ctx = createInitialContext();
      qc = createXAConnection(ctx, lookup, start);
    } catch (Exception e)
    {
      fail("create queue connection failed: " + e);
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
      fail("get queue failed: " + e);
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
