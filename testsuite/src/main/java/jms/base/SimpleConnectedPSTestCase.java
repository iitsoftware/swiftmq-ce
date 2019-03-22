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

import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

public class SimpleConnectedPSTestCase extends PSTestCase
{
  static int _cid = 0;
  public TopicConnection tc = null;
  public TopicSession ts = null;
  public TopicPublisher publisher = null;
  public TopicSubscriber subscriber = null;
  public Topic topic = null;
  boolean isDurable = false;

  public static synchronized String nextCID()
  {
    return "PSTest-" + (_cid++);
  }

  public SimpleConnectedPSTestCase(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.AUTO_ACKNOWLEDGE);
  }

  protected void setUp(boolean transacted, int ackMode) throws Exception
  {
    setUp(transacted, ackMode, false);
  }


  protected void setUp(boolean transacted, int ackMode, boolean isDurable) throws Exception
  {
    setUp(transacted, ackMode, isDurable, true, true);
  }

  protected void setUp(boolean transacted, int ackMode, boolean isDurable, boolean createPublisher, boolean createSubscriber) throws Exception
  {
    this.isDurable = isDurable;
    String tcfName = System.getProperty("jndi.tcf");
    assertNotNull("missing property 'jndi.tcf'", tcfName);
    tc = createTopicConnection(tcfName, isDurable ? (nextCID()) : null);
    String topicName = System.getProperty("jndi.topic");
    assertNotNull("missing property 'jndi.topic'", topicName);
    topic = getTopic(topicName);
    ts = tc.createTopicSession(transacted, ackMode);
    if (createPublisher)
      publisher = ts.createPublisher(topic);
    if (createSubscriber)
      subscriber = isDurable ? ts.createDurableSubscriber(topic, "dur") : ts.createSubscriber(topic);
    tc.start();
  }

  protected void tearDown() throws Exception
  {
    if (subscriber != null)
    {
      subscriber.close();
      if (isDurable)
        ts.unsubscribe("dur");
    }
    if (publisher != null)
      publisher.close();
    ts.close();
    tc.close();
    tc = null;
    ts = null;
    publisher = null;
    subscriber = null;
    topic = null;
    super.tearDown();
  }

}

