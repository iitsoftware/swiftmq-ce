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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Topic;
import javax.naming.InitialContext;

public class UnifiedPSTestCase extends JMSTestCase
{
  public InitialContext ctx = null;
  ConnectionFactory cf = null;

  public UnifiedPSTestCase(String name)
  {
    super(name);
  }

  public Connection createConnection(InitialContext ctx, String lookup)
  {
    return createConnection(ctx, lookup, null);
  }

  public Connection createConnection(InitialContext ctx, String lookup, String clientId, boolean start)
  {
    Connection tc = null;
    try
    {
      if (cf == null)
        cf = (ConnectionFactory) ctx.lookup(lookup);
      tc = cf.createConnection();
      if (clientId != null)
        tc.setClientID(clientId);
      if (start)
        tc.start();
    } catch (Exception e)
    {
      fail("create topic connection failed: " + e);
    }
    return tc;
  }

  public Connection createConnection(InitialContext ctx, String lookup, String clientId)
  {
    return createConnection(ctx, lookup, clientId, true);
  }

  public Connection createConnection(String lookup, String clientId, boolean start)
  {
    Connection tc = null;
    try
    {
      if (ctx == null)
        ctx = createInitialContext();
      tc = createConnection(ctx, lookup, clientId, start);
    } catch (Exception e)
    {
      fail("create topic connection failed: " + e);
    }
    return tc;
  }

  public Connection createConnection(String lookup, String clientId)
  {
    Connection tc = null;
    try
    {
      if (ctx == null)
        ctx = createInitialContext();
      tc = createConnection(ctx, lookup, clientId, true);
    } catch (Exception e)
    {
      fail("create topic connection failed: " + e);
    }
    return tc;
  }

  public Connection createConnection(String lookup, boolean start)
  {
    return createConnection(lookup, null, start);
  }

  public Connection createConnection(String lookup)
  {
    return createConnection(lookup, null, true);
  }

  public Topic getTopic(String name)
  {
    Topic topic = null;
    try
    {
      if (ctx == null)
        ctx = createInitialContext();
      topic = (Topic) ctx.lookup(name);
    } catch (Exception e)
    {
      fail("get topic failed: " + e);
    }
    return topic;
  }

  protected void tearDown() throws Exception
  {
    if (ctx != null)
      ctx.close();
    ctx = null;
    cf = null;
    super.tearDown();
  }
}

