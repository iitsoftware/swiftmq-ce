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

import javax.jms.Topic;
import javax.jms.XATopicConnection;
import javax.jms.XATopicConnectionFactory;
import javax.naming.InitialContext;

public class XAPSTestCase extends JMSTestCase
{
  public InitialContext ctx = null;
  XATopicConnectionFactory tcf = null;

  public XAPSTestCase(String name)
  {
    super(name);
  }

  public XATopicConnection createXATopicConnection(InitialContext ctx, String lookup, String clientId)
  {
    return createXATopicConnection(ctx, lookup, clientId, true);
  }

  public XATopicConnection createXATopicConnection(InitialContext ctx, String lookup, String clientId, boolean start)
  {
    XATopicConnection tc = null;
    try
    {
      if (tcf == null)
        tcf = (XATopicConnectionFactory) ctx.lookup(lookup);
      tc = tcf.createXATopicConnection();
      if (clientId != null)
        tc.setClientID(clientId);
      if (start)
        tc.start();
    } catch (Exception e)
    {
      fail("create xa topic connection failed: " + e);
    }
    return tc;
  }

  public XATopicConnection createXATopicConnection(String lookup, String clientId)
  {
    return createXATopicConnection(lookup, clientId, true);
  }

  public XATopicConnection createXATopicConnection(String lookup, String clientId, boolean start)
  {
    XATopicConnection tc = null;
    try
    {
      if (ctx == null)
        ctx = createInitialContext();
      tc = createXATopicConnection(ctx, lookup, clientId, start);
    } catch (Exception e)
    {
      fail("create topic connection failed: " + e);
    }
    return tc;
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
    tcf = null;
    super.tearDown();
  }
}
