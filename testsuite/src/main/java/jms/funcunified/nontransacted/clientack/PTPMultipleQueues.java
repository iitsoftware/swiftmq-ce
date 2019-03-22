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

import jms.base.SimpleConnectedUnifiedPTPTestCase;

import javax.jms.*;

public class PTPMultipleQueues extends SimpleConnectedUnifiedPTPTestCase
{
  Queue m1 = null;
  Queue m2 = null;
  Queue m3 = null;
  Queue m4 = null;
  Queue m5 = null;
  MessageProducer uiproducer = null;
  MessageProducer qsm1 = null;
  MessageProducer qsm2 = null;
  MessageProducer qsm3 = null;
  MessageProducer qsm4 = null;
  MessageProducer qsm5 = null;
  MessageConsumer qrm1 = null;
  MessageConsumer qrm2 = null;
  MessageConsumer qrm3 = null;
  MessageConsumer qrm4 = null;
  MessageConsumer qrm5 = null;

  public PTPMultipleQueues(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.CLIENT_ACKNOWLEDGE);
    createQueue("m1");
    createQueue("m2");
    createQueue("m3");
    createQueue("m4");
    createQueue("m5");
    m1 = (Queue) ctx.lookup("m1@router");
    m2 = (Queue) ctx.lookup("m2@router");
    m3 = (Queue) ctx.lookup("m3@router");
    m4 = (Queue) ctx.lookup("m4@router");
    m5 = (Queue) ctx.lookup("m5@router");
    uiproducer = qs.createProducer(null);
    qsm1 = qs.createProducer(m1);
    qsm2 = qs.createProducer(m2);
    qsm3 = qs.createProducer(m3);
    qsm4 = qs.createProducer(m4);
    qsm5 = qs.createProducer(m5);
    qrm1 = qs.createConsumer(m1);
    qrm2 = qs.createConsumer(m2);
    qrm3 = qs.createConsumer(m3);
    qrm4 = qs.createConsumer(m4);
    qrm5 = qs.createConsumer(m5);
  }

  public void testPTPUnidentifiedNP()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        uiproducer.send(m1, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uiproducer.send(m2, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uiproducer.send(m3, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uiproducer.send(m4, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uiproducer.send(m5, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm3.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm5.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      msg = (TextMessage) qrm1.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm2.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm3.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm4.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm5.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testPTPIdentifiedNP()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        qsm1.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        qsm2.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        qsm3.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        qsm4.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        qsm5.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm3.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm5.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      msg = (TextMessage) qrm1.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm2.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm3.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm4.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm5.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testPTPUnidentifiedP()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        uiproducer.send(m1, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uiproducer.send(m2, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uiproducer.send(m3, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uiproducer.send(m4, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        uiproducer.send(m5, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm3.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm5.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      msg = (TextMessage) qrm1.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm2.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm3.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm4.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm5.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testPTPIdentifiedP()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        qsm1.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        qsm2.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        qsm3.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        qsm4.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        qsm5.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm3.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm5.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();

      msg = (TextMessage) qrm1.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm2.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm3.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm4.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm5.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testPTPSendReceiveNP()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        qsm1.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        qsm2.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        qsm3.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        qsm4.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        qsm5.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm3.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm5.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }


      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) consumer.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();


      msg = (TextMessage) qrm1.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm2.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm3.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm4.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm5.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testPTPSendReceiveP()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        qsm1.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        qsm2.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        qsm3.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        qsm4.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        qsm5.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm3.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qrm5.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }


      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) consumer.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      msg.acknowledge();


      msg = (TextMessage) qrm1.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm2.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm3.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm4.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) qrm5.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  protected void tearDown() throws Exception
  {
    uiproducer.close();
    qsm1.close();
    qsm2.close();
    qsm3.close();
    qsm4.close();
    qsm5.close();
    qrm1.close();
    qrm2.close();
    qrm3.close();
    qrm4.close();
    qrm5.close();
    deleteQueue("m1");
    deleteQueue("m2");
    deleteQueue("m3");
    deleteQueue("m4");
    deleteQueue("m5");
    super.tearDown();
  }
}

