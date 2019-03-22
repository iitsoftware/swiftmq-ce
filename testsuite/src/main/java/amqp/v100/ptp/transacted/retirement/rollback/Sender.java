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

package amqp.v100.ptp.transacted.retirement.rollback;

import com.swiftmq.amqp.v100.client.Producer;
import com.swiftmq.amqp.v100.client.TransactionController;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TxnIdIF;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import amqp.v100.base.AMQPConnectedSessionTestCase;
import amqp.v100.base.MessageFactory;

public class Sender extends AMQPConnectedSessionTestCase
{
  int nMsgs = Integer.parseInt(System.getProperty("nmsgs", "100000"));
  int txSendSize = Integer.parseInt(System.getProperty("txsendsize", "10"));
  boolean persistent = Boolean.parseBoolean(System.getProperty("persistent", "true"));

  MessageFactory messageFactory;
  int qos;
  String address = null;
  Producer producer = null;
  TransactionController txc = null;

  public Sender(String name, int qos, String address)
  {
    super(name);
    this.qos = qos;
    this.address = address;
  }

  protected void setUp() throws Exception
  {
    super.setUp();
    producer = getSession().createProducer(address, qos);
    messageFactory = (MessageFactory) Class.forName(System.getProperty("messagefactory", "amqp.v100.base.AMQPValueStringMessageFactory")).newInstance();
    txc = getSession().getTransactionController();
  }

  public void send()
  {
    try
    {
      int txSize = 0;
      TxnIdIF txnIdIF = txc.createTxnId();
      boolean rollback = false;
      int i = 0;
      while (i < nMsgs)
      {
        AMQPMessage msg = messageFactory.create(i);
        msg.setTxnIdIF(txnIdIF);
        producer.send(msg, persistent, 5, -1);
        txSize++;
        if (txSize == txSendSize)
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
          txSize = 0;
        }
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
    if (producer != null)
      producer.close();
    super.tearDown();
  }
}
