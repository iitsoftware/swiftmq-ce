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

package jms.func.selector;

import jms.base.SimpleConnectedPSTestCase;

import javax.jms.*;

public class PS extends SimpleConnectedPSTestCase
{
  TopicSubscriber tsub1 = null;
  TopicSubscriber tsub2 = null;
  TopicSubscriber tsub3 = null;
  TopicSubscriber tsub4 = null;
  TopicSubscriber tsub5 = null;
  TopicSubscriber tsub6 = null;

  public PS(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.AUTO_ACKNOWLEDGE);
    tsub1 = ts.createSubscriber(topic, "Int1 between 90 and 120 and time >= " + System.currentTimeMillis(), false);
    tsub2 = ts.createSubscriber(topic, "D1 > 1.0 and D1 < 1000.0", false);
    tsub3 = ts.createSubscriber(topic, "Int2=200 and Int1=400 and S1 like 'Moin%'", false);
    tsub4 = ts.createSubscriber(topic, "D1 > 1.0 and D1 < 1000.0 and S1 in ('Moin', 'MOIN', 'Moin Moin')", false);
    tsub5 = ts.createSubscriber(topic, "prop = 'Nation/World'", false);
    tsub6 = ts.createSubscriber(topic, "prop = 'Nation''s World'", false);
  }

  protected void tearDown() throws Exception
  {
    tsub1.close();
    tsub2.close();
    tsub3.close();
    tsub4.close();
    tsub5.close();
    tsub6.close();
    super.tearDown();
  }

  public void testTS1NP()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        publisher.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsub1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  public void testTS2NP()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        publisher.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsub2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  public void testTS3NP()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        publisher.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      msg = (TextMessage) tsub3.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  public void testTS4NP()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        publisher.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsub4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  public void testTS1P()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        publisher.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsub1.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  public void testTS2P()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        publisher.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsub2.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  public void testTS3P()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        publisher.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      msg = (TextMessage) tsub3.receive(2000);
      assertTrue("Received msg!=null", msg == null);

    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  public void testTS4P()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      msg.setIntProperty("Int1", 100);
      msg.setIntProperty("Int2", 200);
      msg.setIntProperty("Int3", 300);
      msg.setStringProperty("S1", "Moin Moin");
      msg.setLongProperty("time", System.currentTimeMillis());
      msg.setDoubleProperty("D1", 88.99);
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        publisher.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) tsub4.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  public void testTS5P()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      msg.setBooleanProperty("ok",true);
      msg.setStringProperty("prop", "Nation/World");
      msg.setText("testTS5P: 1");
      publisher.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      msg = ts.createTextMessage();
      msg.setBooleanProperty("ok",false);
      msg.setStringProperty("prop", "Nation/World ");
      msg.setText("testTS5P: 2");
      publisher.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      msg = ts.createTextMessage();
      msg.setBooleanProperty("ok",true);
      msg.setStringProperty("prop", "Nation's World");
      msg.setText("testTS5P: 3");
      publisher.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      msg = ts.createTextMessage();
      msg.setBooleanProperty("ok",false);
      msg.setStringProperty("prop", "Nation's' World");
      msg.setText("testTS5P: 4");
      publisher.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

      int cnt = 0;
      msg = (TextMessage) tsub5.receive(2000);
      System.out.println("tsub5: "+msg);
      while (msg != null)
      {
        boolean ok = msg.getBooleanProperty("ok");
        assertTrue("Received msg is !ok", ok);
        cnt++;
        msg = (TextMessage) tsub5.receive(2000);
        System.out.println("tsub5: "+msg);
      }
      assertTrue("cnt != 1",cnt == 1);
      cnt = 0;
      msg = (TextMessage) tsub6.receive(2000);
      System.out.println("tsub6: "+msg);
      while (msg != null)
      {
        boolean ok = msg.getBooleanProperty("ok");
        assertTrue("Received msg is !ok", ok);
        cnt++;
        msg = (TextMessage) tsub6.receive(2000);
        System.out.println("tsub6: "+msg);
      }
      assertTrue("cnt != 1",cnt == 1);
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }
}

