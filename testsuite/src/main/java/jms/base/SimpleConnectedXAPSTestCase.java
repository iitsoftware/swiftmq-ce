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

public class SimpleConnectedXAPSTestCase extends XAPSTestCase
{
  public XATopicConnection tc = null;
  public XATopicSession ts = null;
  public TopicPublisher publisher = null;
  public TopicSubscriber subscriber = null;
  public Topic topic = null;
  public TopicPublisher[] addPublisher = null;
  public TopicSubscriber[] addSubscriber = null;
  public XAResource xares = null;

  public SimpleConnectedXAPSTestCase(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(0);
  }

  protected void setUp(int additionalTopics) throws Exception
  {
    String tcfName = System.getProperty("jndi.tcf");
    assertNotNull("missing property 'jndi.tcf'", tcfName);
    tc = createXATopicConnection(tcfName, "XAPSTest-" + nextId());
    String topicName = System.getProperty("jndi.topic");
    assertNotNull("missing property 'jndi.topic'", topicName);
    topic = getTopic(topicName);
    ts = tc.createXATopicSession();
    publisher = ts.getTopicSession().createPublisher(topic);
    subscriber = ts.getTopicSession().createDurableSubscriber(topic, "dur");
    if (additionalTopics > 0)
    {
      addPublisher = new TopicPublisher[additionalTopics];
      for (int i = 0; i < additionalTopics; i++)
      {
        addPublisher[i] = ts.getTopicSession().createPublisher(getTopic(topicName + i));
      }
      addSubscriber = new TopicSubscriber[additionalTopics];
      for (int i = 0; i < additionalTopics; i++)
      {
        addSubscriber[i] = ts.getTopicSession().createDurableSubscriber(getTopic(topicName + i), "dur" + i);
      }
    }
    xares = ts.getXAResource();
    tc.start();
  }

  protected void setUp(boolean createSubscriber, boolean createPublisher) throws Exception
  {
    String tcfName = System.getProperty("jndi.tcf");
    assertNotNull("missing property 'jndi.tcf'", tcfName);
    tc = createXATopicConnection(tcfName, "XAPSTest-" + nextId());
    String topicName = System.getProperty("jndi.topic");
    assertNotNull("missing property 'jndi.topic'", topicName);
    topic = getTopic(topicName);
    ts = tc.createXATopicSession();
    if (createPublisher)
      publisher = ts.getTopicSession().createPublisher(topic);
    if (createSubscriber)
      subscriber = ts.getTopicSession().createDurableSubscriber(topic, "dur");
    xares = ts.getXAResource();
    tc.start();
  }

  protected void tearDown() throws Exception
  {
    if (subscriber != null)
      subscriber.close();
    if (publisher != null)
      publisher.close();
    if (addPublisher != null)
    {
      for (int i = 0; i < addPublisher.length; i++)
      {
        addPublisher[i].close();
      }
    }
    if (addSubscriber != null)
    {
      for (int i = 0; i < addSubscriber.length; i++)
      {
        addSubscriber[i].close();
        ts.getTopicSession().unsubscribe("dur" + i);
      }
    }
    if (subscriber != null)
      ts.getTopicSession().unsubscribe("dur");
    ts.close();
    tc.close();
    tc = null;
    ts = null;
    publisher = null;
    subscriber = null;
    topic = null;
    addPublisher = null;
    addSubscriber = null;
    xares = null;
    super.tearDown();
  }

}

