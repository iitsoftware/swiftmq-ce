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

package jms.func.transacted;

import jms.base.SimpleConnectedPSTestCase;

import javax.jms.*;

public class PSMultipleTopics extends SimpleConnectedPSTestCase
{
  Topic t1 = null;
  Topic t2 = null;
  Topic t3 = null;
  Topic t4 = null;
  Topic t5 = null;
  TopicPublisher uipublisher = null;
  TopicPublisher tst1 = null;
  TopicPublisher tst2 = null;
  TopicPublisher tst3 = null;
  TopicPublisher tst4 = null;
  TopicPublisher tst5 = null;
  TopicSubscriber tsubt1 = null;
  TopicSubscriber tsubt2 = null;
  TopicSubscriber tsubt3 = null;
  TopicSubscriber tsubt4 = null;
  TopicSubscriber tsubt5 = null;

  public PSMultipleTopics(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(true, Session.CLIENT_ACKNOWLEDGE);
    createTopic("t1");
    createTopic("t2");
    createTopic("t3");
    createTopic("t4");
    createTopic("t5");
    t1 = (Topic) ctx.lookup("t1");
    t2 = (Topic) ctx.lookup("t2");
    t3 = (Topic) ctx.lookup("t3");
    t4 = (Topic) ctx.lookup("t4");
    t5 = (Topic) ctx.lookup("t5");
    uipublisher = ts.createPublisher(null);
    tst1 = ts.createPublisher(t1);
    tst2 = ts.createPublisher(t2);
    tst3 = ts.createPublisher(t3);
    tst4 = ts.createPublisher(t4);
    tst5 = ts.createPublisher(t5);
    tsubt1 = ts.createSubscriber(t1);
    tsubt2 = ts.createSubscriber(t2);
    tsubt3 = ts.createSubscriber(t3);
    tsubt4 = ts.createSubscriber(t4);
    tsubt5 = ts.createSubscriber(t5);
  }

  public void testPSCommitUnidentifiedNP()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        uipublisher.publish(t1, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uipublisher.publish(t2, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uipublisher.publish(t3, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uipublisher.publish(t4, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uipublisher.publish(t5, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      ts.commit();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt3.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt5.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      ts.commit();
      msg = (TextMessage) tsubt1.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt2.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt3.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt4.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt5.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testPSCommitIdentifiedNP()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        tst1.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst2.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst3.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst4.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst5.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      ts.commit();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt3.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt5.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      ts.commit();
      msg = (TextMessage) tsubt1.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt2.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt3.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt4.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt5.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testPSCommitUnidentifiedP()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        uipublisher.publish(t1, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uipublisher.publish(t2, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uipublisher.publish(t3, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uipublisher.publish(t4, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uipublisher.publish(t5, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      ts.commit();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt3.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt5.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      ts.commit();
      msg = (TextMessage) tsubt1.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt2.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt3.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt4.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt5.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testPSCommitIdentifiedP()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        tst1.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst2.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst3.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst4.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst5.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      ts.commit();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt3.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt5.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      ts.commit();
      msg = (TextMessage) tsubt1.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt2.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt3.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt4.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt5.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testPSCommitSendReceiveNP()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        tst1.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst2.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst3.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst4.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst5.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      ts.commit();

      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        publisher.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt3.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt5.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      ts.commit();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) subscriber.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      ts.commit();

      msg = (TextMessage) tsubt1.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt2.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt3.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt4.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt5.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testPSCommitSendReceiveP()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        tst1.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst2.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst3.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst4.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst5.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      ts.commit();

      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        publisher.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt3.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt5.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      ts.commit();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) subscriber.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      ts.commit();

      msg = (TextMessage) tsubt1.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt2.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt3.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt4.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) tsubt5.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testPSRollbackSendReceiveNP()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        tst1.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst2.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst3.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst4.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst5.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      ts.commit();

      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        publisher.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt3.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt5.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      ts.rollback();

      msg = (TextMessage) subscriber.receive(2000);
      assertTrue("Received msg!=null", msg == null);


      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt1.receive(2000);
        assertTrue("Received msg==null", msg != null);
        boolean redelivered = msg.getJMSRedelivered();
        assertTrue("Msg not marked as redelivered", redelivered);
        int cnt = msg.getIntProperty("JMSXDeliveryCount");
        assertTrue("Invalid delivery count: " + cnt, cnt == 2);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt2.receive(2000);
        assertTrue("Received msg==null", msg != null);
        boolean redelivered = msg.getJMSRedelivered();
        assertTrue("Msg not marked as redelivered", redelivered);
        int cnt = msg.getIntProperty("JMSXDeliveryCount");
        assertTrue("Invalid delivery count: " + cnt, cnt == 2);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt3.receive(2000);
        assertTrue("Received msg==null", msg != null);
        boolean redelivered = msg.getJMSRedelivered();
        assertTrue("Msg not marked as redelivered", redelivered);
        int cnt = msg.getIntProperty("JMSXDeliveryCount");
        assertTrue("Invalid delivery count: " + cnt, cnt == 2);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt4.receive(2000);
        assertTrue("Received msg==null", msg != null);
        boolean redelivered = msg.getJMSRedelivered();
        assertTrue("Msg not marked as redelivered", redelivered);
        int cnt = msg.getIntProperty("JMSXDeliveryCount");
        assertTrue("Invalid delivery count: " + cnt, cnt == 2);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt5.receive(2000);
        assertTrue("Received msg==null", msg != null);
        boolean redelivered = msg.getJMSRedelivered();
        assertTrue("Msg not marked as redelivered", redelivered);
        int cnt = msg.getIntProperty("JMSXDeliveryCount");
        assertTrue("Invalid delivery count: " + cnt, cnt == 2);
      }
      ts.commit();
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testPSRollbackSendReceiveP()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        tst1.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst2.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst3.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst4.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst5.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      ts.commit();

      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        publisher.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt3.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt5.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      ts.rollback();

      msg = (TextMessage) subscriber.receive(2000);
      assertTrue("Received msg!=null", msg == null);


      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt1.receive(2000);
        assertTrue("Received msg==null", msg != null);
        boolean redelivered = msg.getJMSRedelivered();
        assertTrue("Msg not marked as redelivered", redelivered);
        int cnt = msg.getIntProperty("JMSXDeliveryCount");
        assertTrue("Invalid delivery count: " + cnt, cnt == 2);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt2.receive(2000);
        assertTrue("Received msg==null", msg != null);
        boolean redelivered = msg.getJMSRedelivered();
        assertTrue("Msg not marked as redelivered", redelivered);
        int cnt = msg.getIntProperty("JMSXDeliveryCount");
        assertTrue("Invalid delivery count: " + cnt, cnt == 2);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt3.receive(2000);
        assertTrue("Received msg==null", msg != null);
        boolean redelivered = msg.getJMSRedelivered();
        assertTrue("Msg not marked as redelivered", redelivered);
        int cnt = msg.getIntProperty("JMSXDeliveryCount");
        assertTrue("Invalid delivery count: " + cnt, cnt == 2);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt4.receive(2000);
        assertTrue("Received msg==null", msg != null);
        boolean redelivered = msg.getJMSRedelivered();
        assertTrue("Msg not marked as redelivered", redelivered);
        int cnt = msg.getIntProperty("JMSXDeliveryCount");
        assertTrue("Invalid delivery count: " + cnt, cnt == 2);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt5.receive(2000);
        assertTrue("Received msg==null", msg != null);
        boolean redelivered = msg.getJMSRedelivered();
        assertTrue("Msg not marked as redelivered", redelivered);
        int cnt = msg.getIntProperty("JMSXDeliveryCount");
        assertTrue("Invalid delivery count: " + cnt, cnt == 2);
      }
      ts.commit();
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  protected void tearDown() throws Exception
  {
    uipublisher.close();
    tst1.close();
    tst2.close();
    tst3.close();
    tst4.close();
    tst5.close();
    tsubt1.close();
    tsubt2.close();
    tsubt3.close();
    tsubt4.close();
    tsubt5.close();
    deleteTopic("t1");
    deleteTopic("t2");
    deleteTopic("t3");
    deleteTopic("t4");
    deleteTopic("t5");
    super.tearDown();
  }
}

