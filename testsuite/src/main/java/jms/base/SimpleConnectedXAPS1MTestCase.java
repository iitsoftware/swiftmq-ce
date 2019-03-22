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
import javax.jms.TopicPublisher;
import javax.jms.TopicSubscriber;
import javax.jms.XATopicConnection;
import javax.jms.XATopicSession;
import javax.transaction.xa.XAResource;

public class SimpleConnectedXAPS1MTestCase extends XAPSTestCase
{
  public XATopicConnection tc = null;
  public XATopicSession ts = null;
  public TopicPublisher publisher = null;
  public Topic topic = null;
  public TopicSubscriber[] subscriber = null;
  public XAResource xares = null;

  public SimpleConnectedXAPS1MTestCase(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(3);
  }

  protected void setUp(int subscribers) throws Exception
  {
    String tcfName = System.getProperty("jndi.tcf");
    assertNotNull("missing property 'jndi.tcf'", tcfName);
    tc = createXATopicConnection(tcfName, "XAPSTest-" + System.currentTimeMillis());
    String topicName = System.getProperty("jndi.topic");
    assertNotNull("missing property 'jndi.topic'", topicName);
    topic = getTopic(topicName);
    ts = tc.createXATopicSession();
    publisher = ts.getTopicSession().createPublisher(topic);
    subscriber = new TopicSubscriber[subscribers];
    for (int i = 0; i < subscribers; i++)
    {
      subscriber[i] = ts.getTopicSession().createDurableSubscriber(getTopic(topicName), "dur" + i);
    }
    xares = ts.getXAResource();
    tc.start();
  }

  protected void tearDown() throws Exception
  {
    publisher.close();
    if (subscriber != null)
    {
      for (int i = 0; i < subscriber.length; i++)
      {
        subscriber[i].close();
        ts.getTopicSession().unsubscribe("dur" + i);
      }
    }
    ts.close();
    tc.close();
    tc = null;
    ts = null;
    publisher = null;
    topic = null;
    subscriber = null;
    xares = null;
    super.tearDown();
  }

}

