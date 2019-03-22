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

import com.swiftmq.amqp.v100.client.Consumer;
import com.swiftmq.amqp.v100.client.TransactionController;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TxnIdIF;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import amqp.v100.base.AMQPConnectedSessionTestCase;
import amqp.v100.base.MessageFactory;

import java.util.ArrayList;
import java.util.List;

public class Receiver extends AMQPConnectedSessionTestCase
{
  int nMsgs = Integer.parseInt(System.getProperty("nmsgs", "100000"));
  int txRcvSize = Integer.parseInt(System.getProperty("txrcvsize", "10"));
  int linkCredit = Integer.parseInt(System.getProperty("linkcredit", "500"));
  List messageList = new ArrayList();

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
    consumer = getSession().createConsumer(address, linkCredit, qos, true, null);
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
      int i = 0;
      while (i < nMsgs)
      {
        AMQPMessage msg = consumer.receive();
        if (msg != null)
        {
          messageFactory.verify(msg);
          messageList.add(msg);
          msg.setTxnIdIF(txnIdIF);
          if (!msg.isSettled())
            msg.accept();
          txSize++;
          if (txSize == txRcvSize)
          {
            if (rollback)
            {
              txc.rollback(txnIdIF);
              TxnIdIF tx2 = txc.createTxnId();
              for (int j = 0; j < messageList.size(); j++)
              {
                AMQPMessage m2 = (AMQPMessage) messageList.get(j);
                m2.setTxnIdIF(tx2);
                if (!m2.isSettled())
                  m2.reject();                                    // forces redelivery
              }
              txc.commit(tx2);
              messageList.clear();
            } else
            {
              txc.commit(txnIdIF);
              i += txSize;
            }
            rollback = !rollback;
            txnIdIF = txc.createTxnId();
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
