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

package amqp.v100.ptp.transacted.acquisition.rollback.requestreply;

import com.swiftmq.amqp.v100.client.Consumer;
import com.swiftmq.amqp.v100.client.Producer;
import com.swiftmq.amqp.v100.client.TransactionController;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AddressIF;
import com.swiftmq.amqp.v100.generated.messaging.message_format.Properties;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TxnIdIF;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import amqp.v100.base.AMQPConnectedSessionTestCase;
import amqp.v100.base.MessageFactory;

public class Replier extends AMQPConnectedSessionTestCase
{
  int nMsgs = Integer.parseInt(System.getProperty("nmsgs", "100000"));

  MessageFactory messageFactory;
  int qos;
  String address = null;
  Consumer consumer = null;
  TransactionController txc = null;

  public Replier(String name, int qos, String address)
  {
    super(name);
    this.qos = qos;
    this.address = address;
  }

  protected void setUp() throws Exception
  {
    super.setUp();
    consumer = getSession().createConsumer(address, qos, true, null);
    messageFactory = (MessageFactory) Class.forName(System.getProperty("messagefactory", "amqp.v100.base.AMQPValueStringMessageFactory")).newInstance();
    txc = getSession().getTransactionController();
  }

  public void serviceRequests()
  {
    try
    {
      boolean rollback = false;
      int i = 0;
      while (i < nMsgs)
      {
        TxnIdIF txnIdIF = txc.createTxnId();
        consumer.acquire(1, txnIdIF);
        AMQPMessage request = consumer.receive();
        if (request != null)
        {
          messageFactory.verify(request);
          if (!request.isSettled())
            request.accept();
          Properties prop = request.getProperties();
          if (prop == null)
            throw new Exception("Properties not set in request: " + request);
          AddressIF replyTo = prop.getReplyTo();
          if (replyTo == null)
            throw new Exception("replyTo not set in request: " + request);
          Producer p = getSession().createProducer(replyTo.getValueString(), qos);
          AMQPMessage reply = messageFactory.createReplyMessage(request);
          reply.setTxnIdIF(txnIdIF);
          Properties prop2 = new Properties();
          prop2.setTo(replyTo);
          prop2.setCorrelationId(prop.getMessageId());
          reply.setProperties(prop2);
          p.send(reply);
          p.close();
        } else
          throw new Exception("Msg == null at i=" + i);
        if (rollback)
          txc.rollback(txnIdIF);
        else
        {
          txc.commit(txnIdIF);
          i++;
        }
        rollback = !rollback;
      }
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  protected void tearDown() throws Exception
  {
    if (consumer != null)
      consumer.close();
    super.tearDown();
  }

}
