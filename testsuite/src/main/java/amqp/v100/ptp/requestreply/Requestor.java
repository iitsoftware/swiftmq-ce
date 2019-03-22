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

package amqp.v100.ptp.requestreply;

import com.swiftmq.amqp.v100.client.Consumer;
import com.swiftmq.amqp.v100.client.Producer;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AddressIF;
import com.swiftmq.amqp.v100.generated.messaging.message_format.Properties;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import amqp.v100.base.AMQPConnectedSessionTestCase;
import amqp.v100.base.MessageFactory;

public class Requestor extends AMQPConnectedSessionTestCase
{
  int linkCredit = Integer.parseInt(System.getProperty("linkcredit", "500"));
  boolean persistent = Boolean.parseBoolean(System.getProperty("persistent", "true"));

  MessageFactory messageFactory;
  int qos;
  String address = null;
  Producer producer = null;
  Consumer consumer = null;
  int nMsgs = 0;

  public Requestor(String name, int qos, String address, int nMsgs)
  {
    super(name);
    this.qos = qos;
    this.address = address;
    this.nMsgs = nMsgs;
  }

  protected void setUp() throws Exception
  {
    super.setUp();
    consumer = getSession().createConsumer(linkCredit, qos);
    producer = getSession().createProducer(address, qos);
    messageFactory = (MessageFactory) Class.forName(System.getProperty("messagefactory", "amqp.v100.base.AMQPValueStringMessageFactory")).newInstance();
  }

  public void sendRequests()
  {
    try
    {
      AddressIF tempDest = consumer.getRemoteAddress();
      for (int i = 0; i < nMsgs; i++)
      {
        AMQPMessage request = messageFactory.create(i);
        Properties prop = new Properties();
        prop.setReplyTo(tempDest);
        request.setProperties(prop);
        producer.send(request, persistent, 5, -1);
        AMQPMessage reply = consumer.receive();
        if (reply == null)
          throw new Exception("Reply is null");
        if (!reply.isSettled())
          reply.accept();
      }
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  protected void tearDown() throws Exception
  {
    if (producer != null)
      producer.close();
    super.tearDown();
  }
}
