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

package com.swiftmq.jms.v630;

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;

import javax.jms.*;
import javax.transaction.xa.XAResource;
import java.io.Serializable;
import java.util.List;

public class XASessionImpl implements XASession
{
  SessionImpl session = null;
  XAResourceImpl xaRes = null;

  XASessionImpl(SessionImpl session)
  {
    this.session = session;
    xaRes = new XAResourceImpl(this);
  }

  public SessionImpl getSessionImpl()
  {
    return session;
  }

  // --> JMS 1.1
  public Session getSession() throws JMSException
  {
    return session;
  }

  public int getAcknowledgeMode() throws JMSException
  {
    return session.getAcknowledgeMode();
  }

  public MessageProducer createProducer(Destination destination) throws JMSException
  {
    return session.createProducer(destination);
  }

  public MessageConsumer createConsumer(Destination destination) throws JMSException
  {
    return session.createConsumer(destination);
  }

  public MessageConsumer createConsumer(Destination destination, String selector) throws JMSException
  {
    return session.createConsumer(destination, selector);
  }

  public MessageConsumer createConsumer(Destination destination, String selector, boolean noLocal) throws JMSException
  {
    return session.createConsumer(destination, selector, noLocal);
  }

  public Queue createQueue(String s) throws JMSException
  {
    return session.createQueue(s);
  }

  public Topic createTopic(String s) throws JMSException
  {
    return session.createTopic(s);
  }

  public TopicSubscriber createDurableSubscriber(Topic topic, String s) throws JMSException
  {
    return session.createDurableSubscriber(topic, s);
  }

  public TopicSubscriber createDurableSubscriber(Topic topic, String s, String s1, boolean b) throws JMSException
  {
    return session.createDurableSubscriber(topic, s, s1, b);
  }

  public QueueBrowser createBrowser(Queue queue) throws JMSException
  {
    return session.createBrowser(queue);
  }

  public QueueBrowser createBrowser(Queue queue, String s) throws JMSException
  {
    return session.createBrowser(queue, s);
  }

  public TemporaryQueue createTemporaryQueue() throws JMSException
  {
    return session.createTemporaryQueue();
  }

  public TemporaryTopic createTemporaryTopic() throws JMSException
  {
    return session.createTemporaryTopic();
  }

  public void unsubscribe(String s) throws JMSException
  {
    session.unsubscribe(s);
  }

  // <-- JMS 1.1
  public boolean getTransacted() throws JMSException
  {
    return session.getTransacted();
  }

  public void close() throws JMSException
  {
    session.close();
  }

  public BytesMessage createBytesMessage() throws JMSException
  {
    return session.createBytesMessage();
  }

  public MapMessage createMapMessage() throws JMSException
  {
    return session.createMapMessage();
  }

  public Message createMessage() throws JMSException
  {
    return session.createMessage();
  }

  public ObjectMessage createObjectMessage() throws JMSException
  {
    return session.createObjectMessage();
  }

  public ObjectMessage createObjectMessage(Serializable serializable) throws JMSException
  {
    return session.createObjectMessage(serializable);
  }

  public StreamMessage createStreamMessage() throws JMSException
  {
    return session.createStreamMessage();
  }

  public TextMessage createTextMessage() throws JMSException
  {
    return session.createTextMessage();
  }

  public TextMessage createTextMessage(String s) throws JMSException
  {
    return session.createTextMessage(s);
  }

  public void recover() throws JMSException
  {
    session.recover();
  }

  public void run()
  {
    session.run();
  }

  public void setMessageListener(MessageListener listener) throws JMSException
  {
    session.setMessageListener(listener);
  }

  public MessageListener getMessageListener() throws JMSException
  {
    return session.getMessageListener();
  }

  public XAResource getXAResource()
  {
    return xaRes;
  }

  public void commit() throws JMSException
  {
    throw new TransactionInProgressException("commit not allowed for XASessions");
  }

  public void rollback() throws JMSException
  {
    throw new TransactionInProgressException("rollback not allowed for XASessions");
  }

  int getDispatchId()
  {
    return session.dispatchId;
  }

  public List getAndClearCurrentTransaction()
  {
    return session.getAndClearCurrentTransaction();
  }

  Reply request(Request request) throws Exception
  {
    return session.requestBlockable(request);
  }
}
