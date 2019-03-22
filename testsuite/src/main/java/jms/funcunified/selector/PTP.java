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

package jms.funcunified.selector;

import jms.base.SimpleConnectedUnifiedPTPTestCase;

import javax.jms.*;

public class PTP extends SimpleConnectedUnifiedPTPTestCase
{
  MessageConsumer qr1 = null;
  MessageConsumer qr2 = null;
  MessageConsumer qr3 = null;
  MessageConsumer qr4 = null;

  public PTP(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.AUTO_ACKNOWLEDGE);
    qr1 = qs.createConsumer(queue, "Int1 between 90 and 120 and time >= " + System.currentTimeMillis());
    qr2 = qs.createConsumer(queue, "D1 > 1.0 and D1 < 1000.0");
    qr3 = qs.createConsumer(queue, "Int2=200 and Int1=400 and S1 like 'Moin%'");
    qr4 = qs.createConsumer(queue, "D1 > 1.0 and D1 < 1000.0 and S1 in ('Moin', 'MOIN', 'Moin Moin')");
  }

  protected void tearDown() throws Exception
  {
    qr1.close();
    qr2.close();
    qr3.close();
    qr4.close();
    super.tearDown();
  }

  public void testQR1NP()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("testQR1NP: " + i);
        producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qr1.receive(2000);
        assertTrue("Received msg==null, i="+i, msg != null);
      }

      msg = (TextMessage) consumer.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testQR2NP()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("testQR2NP: " + i);
        producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qr2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      msg = (TextMessage) consumer.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testQR3NP()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("testQR3NP: " + i);
        producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      msg = (TextMessage) qr3.receive(2000);
      assertTrue("Received msg!=null", msg == null);

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) consumer.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testQR4NP()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("testQR4NP: " + i);
        producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qr4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      msg = (TextMessage) consumer.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testQR1P()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("testQR1P: " + i);
        producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qr1.receive(2000);
        assertTrue("Received msg==null, i="+i, msg != null);
      }

      msg = (TextMessage) consumer.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testQR2P()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("testQR2P: " + i);
        producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qr2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      msg = (TextMessage) consumer.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testQR3P()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("testQR3P: " + i);
        producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      msg = (TextMessage) qr3.receive(2000);
      assertTrue("Received msg!=null", msg == null);

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) consumer.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testQR4P()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("testQR4P: " + i);
        producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) qr4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      msg = (TextMessage) consumer.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }
}

