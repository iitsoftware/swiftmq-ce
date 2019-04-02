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

public class PTPML extends SimpleConnectedUnifiedPTPTestCase
{
  MessageConsumer qr1 = null;
  MessageConsumer qr2 = null;
  MessageConsumer qr3 = null;
  MessageConsumer qr4 = null;
  int cnt1 = 0;
  int cnt2 = 0;
  int cnt3 = 0;
  int cnt4 = 0;

  public PTPML(String name)
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
      cnt1 = 0;
      qr1.setMessageListener(new MessageListener()
      {
        public void onMessage(Message msg)
        {
          cnt1++;
          System.out.println("received, cnt1=" + cnt1);
        }
      });
      TextMessage msg = qs.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      pause(5000);

      assertTrue("Received cnt1!=10", cnt1 == 10);
      qr1.setMessageListener(null);
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  public void testQR2NP()
  {
    try
    {
      cnt2 = 0;
      qr2.setMessageListener(new MessageListener()
      {
        public void onMessage(Message msg)
        {
          cnt2++;
          System.out.println("received, cnt2=" + cnt2);
        }
      });
      TextMessage msg = qs.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      pause(5000);

      assertTrue("Received cnt2!=10", cnt2 == 10);
      qr2.setMessageListener(null);
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  public void testQR3NP()
  {
    try
    {
      cnt3 = 0;
      qr3.setMessageListener(new MessageListener()
      {
        public void onMessage(Message msg)
        {
          cnt3++;
          System.out.println("received, cnt3=" + cnt3);
        }
      });
      TextMessage msg = qs.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      pause(5000);

      assertTrue("Received cnt3!=0", cnt3 == 0);
      qr3.setMessageListener(null);

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) consumer.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  public void testQR4NP()
  {
    try
    {
      cnt4 = 0;
      qr4.setMessageListener(new MessageListener()
      {
        public void onMessage(Message msg)
        {
          cnt4++;
          System.out.println("received, cnt4=" + cnt4);
        }
      });
      TextMessage msg = qs.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      pause(5000);

      assertTrue("Received cnt4!=10", cnt4 == 10);
      qr4.setMessageListener(null);
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  public void testQR1P()
  {
    try
    {
      cnt1 = 0;
      qr1.setMessageListener(new MessageListener()
      {
        public void onMessage(Message msg)
        {
          cnt1++;
          System.out.println("received, cnt1=" + cnt1);
        }
      });
      TextMessage msg = qs.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      pause(5000);

      assertTrue("Received cnt1!=10", cnt1 == 10);
      qr1.setMessageListener(null);
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  public void testQR2P()
  {
    try
    {
      cnt2 = 0;
      qr2.setMessageListener(new MessageListener()
      {
        public void onMessage(Message msg)
        {
          cnt2++;
          System.out.println("received, cnt2=" + cnt2);
        }
      });
      TextMessage msg = qs.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      pause(5000);

      assertTrue("Received cnt2!=10", cnt2 == 10);
      qr2.setMessageListener(null);
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  public void testQR3P()
  {
    try
    {
      cnt3 = 0;
      qr3.setMessageListener(new MessageListener()
      {
        public void onMessage(Message msg)
        {
          cnt3++;
          System.out.println("received, cnt3=" + cnt3);
        }
      });
      TextMessage msg = qs.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      pause(5000);

      assertTrue("Received cnt3!=0", cnt3 == 0);
      qr3.setMessageListener(null);

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) consumer.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  public void testQR4P()
  {
    try
    {
      cnt4 = 0;
      qr4.setMessageListener(new MessageListener()
      {
        public void onMessage(Message msg)
        {
          cnt4++;
          System.out.println("received, cnt4=" + cnt4);
        }
      });
      TextMessage msg = qs.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      pause(5000);

      assertTrue("Received cnt4!=10", cnt4 == 10);
      qr4.setMessageListener(null);
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }
}

