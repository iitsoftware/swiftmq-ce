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

package jms.funcunified.nontransacted.clientack;

import jms.base.SimpleConnectedUnifiedPSTestCase;

import javax.jms.*;

public class PSMultipleTopicsDur extends SimpleConnectedUnifiedPSTestCase
{
  Topic t1 = null;
  Topic t2 = null;
  Topic t3 = null;
  Topic t4 = null;
  Topic t5 = null;
  MessageProducer uiproducer = null;
  MessageProducer tst1 = null;
  MessageProducer tst2 = null;
  MessageProducer tst3 = null;
  MessageProducer tst4 = null;
  MessageProducer tst5 = null;
  MessageConsumer tsubt1 = null;
  MessageConsumer tsubt2 = null;
  MessageConsumer tsubt3 = null;
  MessageConsumer tsubt4 = null;
  MessageConsumer tsubt5 = null;

  public PSMultipleTopicsDur(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.CLIENT_ACKNOWLEDGE, true);
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
    uiproducer = ts.createProducer(null);
    tst1 = ts.createProducer(t1);
    tst2 = ts.createProducer(t2);
    tst3 = ts.createProducer(t3);
    tst4 = ts.createProducer(t4);
    tst5 = ts.createProducer(t5);
    tsubt1 = ts.createDurableSubscriber(t1, "dur100");
    tsubt2 = ts.createDurableSubscriber(t2, "dur101");
    tsubt3 = ts.createDurableSubscriber(t3, "dur102");
    tsubt4 = ts.createDurableSubscriber(t4, "dur103");
    tsubt5 = ts.createDurableSubscriber(t5, "dur104");
  }

  public void testPSUnidentifiedDurNP()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        uiproducer.send(t1, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uiproducer.send(t2, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uiproducer.send(t3, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uiproducer.send(t4, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uiproducer.send(t5, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt3.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt5.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

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

  public void testPSIdentifiedDurNP()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        tst1.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst2.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst3.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst4.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst5.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt3.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt5.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

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

  public void testPSUnidentifiedDurP()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        uiproducer.send(t1, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uiproducer.send(t2, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uiproducer.send(t3, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uiproducer.send(t4, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uiproducer.send(t5, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt3.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt5.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

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

  public void testPSIdentifiedDurP()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        tst1.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst2.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst3.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst4.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst5.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt3.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt5.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

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

  public void testPSSendReceiveDurNP()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        tst1.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst2.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst3.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst4.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst5.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt3.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt5.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) consumer.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

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

  public void testPSSendReceiveDurP()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        tst1.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst2.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst3.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst4.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        tst5.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt3.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsubt5.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) consumer.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

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

  protected void tearDown() throws Exception
  {
    uiproducer.close();
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
    ts.unsubscribe("dur100");
    ts.unsubscribe("dur101");
    ts.unsubscribe("dur102");
    ts.unsubscribe("dur103");
    ts.unsubscribe("dur104");
    deleteTopic("t1");
    deleteTopic("t2");
    deleteTopic("t3");
    deleteTopic("t4");
    deleteTopic("t5");
    super.tearDown();
  }
}

