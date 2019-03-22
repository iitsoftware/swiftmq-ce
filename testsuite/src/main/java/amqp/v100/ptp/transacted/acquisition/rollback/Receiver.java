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

package amqp.v100.ptp.transacted.acquisition.rollback;

import com.swiftmq.amqp.v100.client.Consumer;
import com.swiftmq.amqp.v100.client.TransactionController;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TxnIdIF;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import amqp.v100.base.AMQPConnectedSessionTestCase;
import amqp.v100.base.MessageFactory;

public class Receiver extends AMQPConnectedSessionTestCase
{
  int nMsgs = Integer.parseInt(System.getProperty("nmsgs", "100000"));
  int txRcvSize = Integer.parseInt(System.getProperty("txrcvsize", "10"));

  MessageFactory messageFactory;
  int qos;
  String address = null;
  Consumer consumer = null;
  TransactionController txc = null;

  public Receiver(String name, int qos, String address)
  {
    super(name);
    this.qos = qos;
    this.address = address;
  }

  protected void setUp() throws Exception
  {
    super.setUp();
    consumer = getSession().createConsumer(address, qos, true, null);          // NO! link credit
    messageFactory = (MessageFactory) Class.forName(System.getProperty("messagefactory", "amqp.v100.base.AMQPValueStringMessageFactory")).newInstance();
    txc = getSession().getTransactionController();
  }

  public void receive()
  {
    try
    {
      int txSize = 0;
      boolean rollback = false;
      TxnIdIF txnIdIF = txc.createTxnId();
      consumer.acquire(txRcvSize, txnIdIF);
      int i = 0;
      while (i < nMsgs)
      {
        AMQPMessage msg = consumer.receive();
        if (msg != null)
        {
          messageFactory.verify(msg);
          if (!msg.isSettled())
            msg.accept();
          txSize++;
          if (txSize == txRcvSize)
          {
            if (rollback)
              txc.rollback(txnIdIF);
            else
            {
              txc.commit(txnIdIF);
              i += txSize;
            }
            rollback = !rollback;
            txnIdIF = txc.createTxnId();
            consumer.acquire(txRcvSize, txnIdIF);
            txSize = 0;
          }
        } else
          throw new Exception("Msg == null at i=" + i);
      }
      if (txSize > 0)
      {
        if (rollback)
          txc.rollback(txnIdIF);
        else
          txc.commit(txnIdIF);
        rollback = !rollback;
        txSize = 0;
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
