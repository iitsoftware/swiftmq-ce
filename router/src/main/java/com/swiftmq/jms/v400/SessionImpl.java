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

package com.swiftmq.jms.v400;

import com.swiftmq.client.thread.PoolManager;
import com.swiftmq.jms.*;
import com.swiftmq.jms.smqp.v400.*;
import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.swiftlet.queue.MessageIndex;
import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.tools.collection.RingBuffer;
import com.swiftmq.tools.queue.SingleProcessorQueue;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestRegistry;
import com.swiftmq.tools.requestreply.RequestService;
import com.swiftmq.tools.requestreply.TimeoutException;
import com.swiftmq.util.SwiftUtilities;

import javax.jms.*;
import javax.jms.IllegalStateException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SessionImpl
    implements Session, RequestService, QueueSession, TopicSession, SwiftMQSession
{
  public static final String DISPATCH_TOKEN = "sys$jms.client.session.sessiontask";

  static final int TYPE_SESSION = 0;
  static final int TYPE_QUEUE_SESSION = 1;
  static final int TYPE_TOPIC_SESSION = 2;

  boolean closed = false;
  boolean transacted = false;
  int acknowledgeMode = 0;
  int dispatchId = 0;
  int myDispatchId = 0;
  String clientId = null;
  RequestRegistry requestRegistry = null;
  ConnectionImpl myConnection = null;
  String myHostname = null;
  String userName = null;
  ExceptionListener exceptionListener = null;
  Map consumerMap = new HashMap();
  int lastConsumerId = -1;
  ArrayList transactedRequestList = new ArrayList();
  MessageListener messageListener = null;
  RingBuffer messageChunk = null;
  boolean shadowConsumerCreated = false;
  MessageEntry lastMessage = null;
  boolean autoAssign = true;
  ThreadPool sessionPool = null;
  SessionDeliveryQueue sessionQueue = null;
  SessionTask sessionTask = null;
  int recoveryEpoche = 0;
  boolean recoveryInProgress = false;
  int type = TYPE_SESSION;
  boolean useThreadContextCL = false;

  protected SessionImpl(int type, ConnectionImpl myConnection, boolean transacted, int acknowledgeMode, int dispatchId, RequestRegistry requestRegistry, String myHostname, String clientId)
  {
    this.type = type;
    this.myConnection = myConnection;
    this.transacted = transacted;
    this.acknowledgeMode = acknowledgeMode;
    this.dispatchId = dispatchId;
    this.requestRegistry = requestRegistry;
    this.myHostname = myHostname;
    this.clientId = clientId;
    this.sessionPool = PoolManager.getInstance().getSessionPool();
    useThreadContextCL = myConnection.isUseThreadContextCL();
    sessionTask = new SessionTask();
    sessionQueue = new SessionDeliveryQueue();
  }

  void startSession()
  {
    sessionQueue.startQueue();
  }

  void stopSession()
  {
    sessionQueue.stopQueue();
  }

  void setUserName(String userName)
  {
    this.userName = userName;
  }

  String getUserName()
  {
    return userName;
  }

  public ConnectionImpl getMyConnection()
  {
    return myConnection;
  }

  void verifyState() throws JMSException
  {
    if (closed)
    {
      throw new javax.jms.IllegalStateException("Session is closed");
    }
  }

  public void storeTransactedRequest(Request req)
  {
    synchronized (transactedRequestList)
    {
      transactedRequestList.add(req);
    }
  }

  public void storeTransactedMessage(int producerId, MessageImpl msg)
  {
    synchronized (transactedRequestList)
    {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(bos);
      try
      {
        dos.writeInt(producerId);
        msg.writeContent(dos);
      } catch (Exception e)
      {
        e.printStackTrace();
      }
      transactedRequestList.add(bos.toByteArray());
    }
  }

  public Reply requestTransaction(CommitRequest req) throws TimeoutException
  {
    synchronized (transactedRequestList)
    {
      req.setMessages((Object[]) transactedRequestList.toArray(new Object[transactedRequestList.size()]));
      transactedRequestList.clear();
    }

    return requestRegistry.request(req);
  }

  public Object[] getAndClearCurrentTransaction()
  {
    Object[] wrapper = null;
    synchronized (transactedRequestList)
    {
      wrapper = transactedRequestList.toArray(new Object[transactedRequestList.size()]);
      transactedRequestList.clear();
    }
    return wrapper;
  }

  public void setCurrentTransaction(Object[] current)
  {
    synchronized (transactedRequestList)
    {
      transactedRequestList.clear();
      if (current != null)
      {
        for (int i = 0; i < current.length; i++)
        {
          transactedRequestList.add(current[i]);
        }
      }
    }
  }

  public void dropTransaction()
  {
    synchronized (transactedRequestList)
    {
      transactedRequestList.clear();
    }
  }

  void setExceptionListener(ExceptionListener exceptionListener)
  {
    this.exceptionListener = exceptionListener;
  }

  void setMyDispatchId(int myDispatchId)
  {
    this.myDispatchId = myDispatchId;
  }

  int getMyDispatchId()
  {
    return myDispatchId;
  }

  synchronized void addMessageConsumerImpl(MessageConsumerImpl consumer)
  {
    if (lastConsumerId == Integer.MAX_VALUE)
      lastConsumerId = -1;
    lastConsumerId++;
    consumerMap.put(new Integer(lastConsumerId), consumer);
    consumer.setConsumerId(lastConsumerId);
  }

  synchronized void removeMessageConsumerImpl(MessageConsumerImpl consumer)
  {
    consumerMap.remove(new Integer(consumer.getConsumerId()));
  }

  // --> JMS 1.1

  public QueueReceiver createReceiver(Queue queue) throws JMSException
  {
    verifyState();

    return (createReceiver(queue, null));
  }

  public synchronized QueueReceiver createReceiver(Queue queue, String messageSelector)
      throws JMSException
  {
    verifyState();

    if (queue == null)
      throw new InvalidDestinationException("createReceiver, queue is null!");

    QueueReceiverImpl qr = null;
    CreateConsumerReply reply = null;

    try
    {
      String ms = messageSelector;
      if (messageSelector != null && messageSelector.trim().length() == 0)
        ms = null;
      reply =
          (CreateConsumerReply) requestRegistry.request(new CreateConsumerRequest(dispatchId,
              (QueueImpl) queue, ms));
    } catch (Exception e)
    {
      throw ExceptionConverter.convert(e);
    }

    if (reply.isOk())
    {
      int qcId = reply.getQueueConsumerId();

      qr = new QueueReceiverImpl(transacted, acknowledgeMode, dispatchId,
          requestRegistry, queue, messageSelector,
          this);

      qr.setServerQueueConsumerId(qcId);
      qr.setDoAck(!transacted && acknowledgeMode != Session.CLIENT_ACKNOWLEDGE);
      addMessageConsumerImpl(qr);
    } else
    {
      throw ExceptionConverter.convert(reply.getException());
    }

    return (qr);
  }

  public QueueSender createSender(Queue queue) throws JMSException
  {
    verifyState();

    // queue can be null = unidentified sender!
    if (queue == null)
      return new QueueSenderImpl(this, queue, dispatchId, -1, requestRegistry, myHostname);

    QueueSenderImpl queueSender = null;
    CreateProducerReply reply = null;

    try
    {
      reply = (CreateProducerReply) requestRegistry.request(new CreateProducerRequest(dispatchId,
          (QueueImpl) queue));
    } catch (Exception e)
    {
      throw ExceptionConverter.convert(e);
    }

    if (reply.isOk())
    {

      // create the sender
      queueSender = new QueueSenderImpl(this, queue, dispatchId, reply.getQueueProducerId(),
          requestRegistry, myHostname);
      queueSender.setDestinationImpl(queue);
    } else
    {
      throw ExceptionConverter.convert(reply.getException());
    }

    return (queueSender);
  }

  public TopicSubscriber createSubscriber(Topic topic)
      throws JMSException
  {
    return createSubscriber(topic, null, false);
  }

  public synchronized TopicSubscriber createSubscriber(Topic topic, String messageSelector, boolean noLocal)
      throws JMSException
  {
    verifyState();

    if (topic == null)
      throw new InvalidDestinationException("createSubscriber, topic is null!");

    TopicSubscriberImpl ts = null;
    CreateSubscriberReply reply = null;

    try
    {
      String ms = messageSelector;
      if (messageSelector != null && messageSelector.trim().length() == 0)
        ms = null;
      reply =
          (CreateSubscriberReply) requestRegistry.request(new CreateSubscriberRequest(dispatchId,
              (TopicImpl) topic, ms, noLocal));
    } catch (Exception e)
    {
      throw ExceptionConverter.convert(e);
    }

    if (reply.isOk())
    {
      int tsId = reply.getTopicSubscriberId();

      ts = new TopicSubscriberImpl(transacted, acknowledgeMode, dispatchId,
          requestRegistry, topic, messageSelector,
          this, noLocal);

      ts.setServerQueueConsumerId(tsId);
      ts.setDoAck(false);
      addMessageConsumerImpl(ts);
    } else
    {
      throw ExceptionConverter.convert(reply.getException());
    }

    return (ts);
  }

  public TopicSubscriber createDurableSubscriber(Topic topic, String name)
      throws JMSException
  {
    return createDurableSubscriber(topic, name, null, false);
  }

  public synchronized TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal)
      throws JMSException
  {
    verifyState();

    if (myConnection.getClientID() == null)
      throw new IllegalStateException("unable to create durable subscriber, no client ID has been set");

    if (topic == null)
      throw new InvalidDestinationException("createDurableSubscriber, topic is null!");

    if (name == null)
      throw new NullPointerException("createDurableSubscriber, name is null!");

    try
    {
      SwiftUtilities.verifyDurableName(name);
    } catch (Exception e)
    {
      throw new JMSException(e.getMessage());
    }

    TopicSubscriberImpl ts = null;
    CreateDurableReply reply = null;

    try
    {
      String ms = messageSelector;
      if (messageSelector != null && messageSelector.trim().length() == 0)
        ms = null;
      reply =
          (CreateDurableReply) requestRegistry.request(new CreateDurableRequest(dispatchId,
              (TopicImpl) topic, ms, noLocal, name));
    } catch (Exception e)
    {
      throw ExceptionConverter.convert(e);
    }

    if (reply.isOk())
    {
      int tsId = reply.getTopicSubscriberId();

      ts = new TopicSubscriberImpl(transacted, acknowledgeMode, dispatchId,
          requestRegistry, topic, messageSelector,
          this, noLocal);

      ts.setServerQueueConsumerId(tsId);
      ts.setDoAck(!transacted && acknowledgeMode != Session.CLIENT_ACKNOWLEDGE);
      addMessageConsumerImpl(ts);
    } else
    {
      throw ExceptionConverter.convert(reply.getException());
    }

    return (ts);
  }

  public TopicPublisher createPublisher(Topic topic)
      throws JMSException
  {
    verifyState();

    // topic can be null = unidentified publisher
    if (topic == null)
      return new TopicPublisherImpl(this, topic, dispatchId, -1, requestRegistry, myHostname, clientId);

    TopicPublisherImpl topicPublisher = null;
    CreatePublisherReply reply = null;

    try
    {
      reply = (CreatePublisherReply) requestRegistry.request(new CreatePublisherRequest(dispatchId,
          (TopicImpl) topic));
    } catch (Exception e)
    {
      throw ExceptionConverter.convert(e);
    }

    if (reply.isOk())
    {

      // create the publisher
      topicPublisher = new TopicPublisherImpl(this, topic, dispatchId, reply.getTopicPublisherId(),
          requestRegistry, myHostname, clientId);
      topicPublisher.setDestinationImpl(topic);
    } else
    {
      throw ExceptionConverter.convert(reply.getException());
    }

    return (topicPublisher);
  }

  public MessageProducer createProducer(Destination destination) throws JMSException
  {
    if (destination == null) // unidentified
      return createSender(null);
    DestinationImpl destImpl = (DestinationImpl) destination;
    MessageProducer producer = null;
    switch (destImpl.getType())
    {
      case DestinationFactory.TYPE_QUEUE:
      case DestinationFactory.TYPE_TEMPQUEUE:
        producer = createSender((Queue) destination);
        break;
      case DestinationFactory.TYPE_TOPIC:
      case DestinationFactory.TYPE_TEMPTOPIC:
        producer = createPublisher((Topic) destination);
        break;
    }
    return producer;
  }

  public MessageConsumer createConsumer(Destination destination) throws JMSException
  {
    return createConsumer(destination, null, false);
  }

  public MessageConsumer createConsumer(Destination destination, String selector) throws JMSException
  {
    return createConsumer(destination, selector, false);
  }

  public MessageConsumer createConsumer(Destination destination, String selector, boolean noLocal) throws JMSException
  {
    if (destination == null)
      throw new InvalidDestinationException("createConsumer, destination is null!");
    DestinationImpl destImpl = (DestinationImpl) destination;
    MessageConsumer consumer = null;
    switch (destImpl.getType())
    {
      case DestinationFactory.TYPE_QUEUE:
      case DestinationFactory.TYPE_TEMPQUEUE:
        consumer = createReceiver((Queue) destination, selector);
        break;
      case DestinationFactory.TYPE_TOPIC:
      case DestinationFactory.TYPE_TEMPTOPIC:
        consumer = createSubscriber((Topic) destination, selector, noLocal);
        break;
    }
    return consumer;
  }

  public Queue createQueue(String queueName) throws JMSException
  {
    verifyState();
    if (type == TYPE_TOPIC_SESSION)
      throw new IllegalStateException("Operation not allowed on this session type");

    if (queueName == null)
      throw new InvalidDestinationException("createQueue, queueName is null!");

    try
    {
      SwiftUtilities.verifyQueueName(queueName);
    } catch (Exception e)
    {
      throw new JMSException(e.getMessage());
    }
    return new QueueImpl(queueName);
  }

  public Topic createTopic(String topicName)
      throws JMSException
  {
    verifyState();
    if (type == TYPE_QUEUE_SESSION)
      throw new IllegalStateException("Operation not allowed on this session type");
    if (topicName == null)
      throw new InvalidDestinationException("createTopic, topicName is null!");

    return new TopicImpl(topicName);
  }

  public QueueBrowser createBrowser(Queue queue) throws JMSException
  {
    verifyState();
    if (type == TYPE_TOPIC_SESSION)
      throw new IllegalStateException("Operation not allowed on this session type");
    return (createBrowser(queue, null));
  }

  public QueueBrowser createBrowser(Queue queue, String messageSelector)
      throws JMSException
  {
    verifyState();
    if (type == TYPE_TOPIC_SESSION)
      throw new IllegalStateException("Operation not allowed on this session type");

    if (queue == null)
      throw new InvalidDestinationException("createBrowser, queue is null!");

    QueueBrowser queueBrowser = null;
    CreateBrowserReply reply = null;

    try
    {
      String ms = messageSelector;
      if (messageSelector != null && messageSelector.trim().length() == 0)
        ms = null;
      reply =
          (CreateBrowserReply) requestRegistry.request(new CreateBrowserRequest(dispatchId,
              (QueueImpl) queue, ms));
    } catch (Exception e)
    {
      throw ExceptionConverter.convert(e);
    }

    if (reply.isOk())
    {

      // create the browser
      queueBrowser = new QueueBrowserImpl(this, queue, messageSelector,
          dispatchId,
          reply.getQueueBrowserId(),
          requestRegistry);
    } else
    {
      throw ExceptionConverter.convert(reply.getException());
    }

    return (queueBrowser);
  }

  public TemporaryQueue createTemporaryQueue() throws JMSException
  {
    verifyState();
    if (type == TYPE_TOPIC_SESSION)
      throw new IllegalStateException("Operation not allowed on this session type");

    TemporaryQueue tempQueue = null;
    CreateTmpQueueReply reply = null;

    try
    {
      reply = (CreateTmpQueueReply) requestRegistry.request(new CreateTmpQueueRequest());
      tempQueue = new TemporaryQueueImpl(reply.getQueueName(), myConnection);
    } catch (Exception e)
    {
      throw ExceptionConverter.convert(e);
    }

    if (!reply.isOk())
    {
      throw ExceptionConverter.convert(reply.getException());
    }

    return tempQueue;
  }

  public TemporaryTopic createTemporaryTopic()
      throws JMSException
  {
    verifyState();
    if (type == TYPE_QUEUE_SESSION)
      throw new IllegalStateException("Operation not allowed on this session type");

    TemporaryTopic tempTopic = null;
    CreateTmpQueueReply reply = null;

    try
    {
      reply = (CreateTmpQueueReply) requestRegistry.request(new CreateTmpQueueRequest());
      tempTopic = new TemporaryTopicImpl(reply.getQueueName(), myConnection);
    } catch (Exception e)
    {
      throw ExceptionConverter.convert(e);
    }

    if (!reply.isOk())
    {
      throw ExceptionConverter.convert(reply.getException());
    }

    return tempTopic;
  }

  public void unsubscribe(String name)
      throws JMSException
  {
    verifyState();
    if (type == TYPE_QUEUE_SESSION)
      throw new IllegalStateException("Operation not allowed on this session type");

    if (name == null)
      throw new NullPointerException("unsubscribe, name is null!");

    DeleteDurableReply reply = null;

    try
    {
      reply =
          (DeleteDurableReply) requestRegistry.request(new DeleteDurableRequest(dispatchId, name));
    } catch (Exception e)
    {
      throw ExceptionConverter.convert(e);
    }

    if (!reply.isOk())
    {
      throw ExceptionConverter.convert(reply.getException());
    }
  }

  public int getAcknowledgeMode() throws JMSException
  {
    verifyState();
    return acknowledgeMode;
  }
  // <-- JMS 1.1

  public BytesMessage createBytesMessage() throws JMSException
  {
    verifyState();

    return (new BytesMessageImpl());
  }

  public MapMessage createMapMessage() throws JMSException
  {
    verifyState();

    return (new MapMessageImpl());
  }

  public Message createMessage() throws JMSException
  {
    verifyState();

    return (new MessageImpl());
  }

  public ObjectMessage createObjectMessage() throws JMSException
  {
    verifyState();

    return (new ObjectMessageImpl());
  }

  public ObjectMessage createObjectMessage(Serializable object)
      throws JMSException
  {
    verifyState();

    ObjectMessage msg = createObjectMessage();

    msg.setObject(object);

    return (msg);
  }

  public StreamMessage createStreamMessage() throws JMSException
  {
    verifyState();

    return (new StreamMessageImpl());     // NYI
  }

  public TextMessage createTextMessage() throws JMSException
  {
    verifyState();

    return new TextMessageImpl();
  }

  public TextMessage createTextMessage(String s)
      throws JMSException
  {
    verifyState();

    TextMessage msg = createTextMessage();

    msg.setText(new String(s));

    return (msg);
  }

  public boolean getTransacted() throws JMSException
  {
    verifyState();

    return (transacted);
  }

  public void commit() throws JMSException
  {
    verifyState();

    if (transacted)
    {

      // will send all produced messages within this transaction and
      // also commit all consumed messages on the server side
      CommitReply reply = null;

      try
      {
        reply = (CommitReply) requestTransaction(new CommitRequest(dispatchId));
      } catch (Exception e)
      {
        throw ExceptionConverter.convert(e);
      }

      if (!reply.isOk())
      {
        throw ExceptionConverter.convert(reply.getException());
      }
      long delay = reply.getDelay();
      if (delay > 0)
      {
        try
        {
          Thread.sleep(delay);
        } catch (Exception ignored)
        {
        }
      }
    } else
    {
      throw new javax.jms.IllegalStateException("Session is not transacted - commit not allowed");
    }
  }

  synchronized void startRecoverConsumers()
  {
    sessionQueue.stopQueue();
    recoveryInProgress = true;
    recoveryEpoche++;
    for (Iterator iter = consumerMap.entrySet().iterator(); iter.hasNext();)
    {
      MessageConsumerImpl c = (MessageConsumerImpl) ((Map.Entry) iter.next()).getValue();
      c.setWasRecovered(true);
      c.clearCache();
    }
    sessionQueue.clear();
  }

  synchronized void endRecoverConsumers()
  {
    recoveryInProgress = false;
    sessionQueue.startQueue();
  }

  public void rollback() throws JMSException
  {
    verifyState();

    if (transacted)
    {
      startRecoverConsumers();

      // drop stored messages on the client side (produced messages)
      dropTransaction();

      // also rollback on client side (for consumed messages)
      Reply reply = null;

      try
      {
        reply = requestRegistry.request(new RollbackRequest(dispatchId));
      } catch (Exception e)
      {
        throw ExceptionConverter.convert(e);
      }

      if (reply.isOk())
      {
        endRecoverConsumers();
      } else
      {
        throw ExceptionConverter.convert(reply.getException());
      }
    } else
    {
      throw new javax.jms.IllegalStateException("Session is not transacted - rollback not allowed");
    }
  }

  boolean isClosed()
  {
    return closed;
  }

  public void close() throws JMSException
  {
    if (closed)
      return;
    sessionQueue.stopQueue();
    sessionQueue.clear();
    Reply reply = null;
    synchronized (this)
    {
      closed = true;
      for (Iterator iter = consumerMap.entrySet().iterator(); iter.hasNext();)
      {
        MessageConsumerImpl consumer = (MessageConsumerImpl) ((Map.Entry) iter.next()).getValue();
        consumer.cancel();
      }
      consumerMap.clear();

      if (transacted)
      {
        dropTransaction();
      }
    }

    try
    {
      reply = requestRegistry.request(new CloseSessionRequest(dispatchId));
    } catch (Exception e)
    {
      throw ExceptionConverter.convert(e);
    }
    myConnection.removeRequestService(myDispatchId);
    myConnection.removeSession(this);
    if (reply == null)
      return;
    if (!reply.isOk())
    {
      throw ExceptionConverter.convert(reply.getException());
    }
  }

  void cancel()
  {
    closed = true;
    sessionQueue.stopQueue();
    sessionQueue.clear();
    for (Iterator iter = consumerMap.entrySet().iterator(); iter.hasNext();)
    {
      MessageConsumerImpl consumer = (MessageConsumerImpl) ((Map.Entry) iter.next()).getValue();
      consumer.cancel();
    }
    consumerMap.clear();

    if (transacted)
    {
      dropTransaction();
    }

  }

  public synchronized void recover() throws JMSException
  {
    verifyState();

    if (!transacted)
    {
      startRecoverConsumers();

      Reply reply = null;
      try
      {
        reply = requestRegistry.request(new RecoverSessionRequest(dispatchId));
      } catch (Exception e)
      {
        throw ExceptionConverter.convert(e);
      }

      if (reply.isOk())
      {
        endRecoverConsumers();
      } else
      {
        throw ExceptionConverter.convert(reply.getException());
      }
    } else
    {
      throw new javax.jms.IllegalStateException("Session is transacted - recover not allowed");
    }
  }

  public MessageListener getMessageListener() throws JMSException
  {
    verifyState();
    return messageListener;
  }

  public synchronized void setMessageListener(MessageListener messageListener) throws JMSException
  {
    verifyState();
    this.messageListener = messageListener;
  }

  boolean isShadowConsumerCreated()
  {
    return shadowConsumerCreated;
  }

  void createShadowConsumer(String queueName) throws Exception
  {
    Reply reply = requestRegistry.request(new CreateShadowConsumerRequest(dispatchId, queueName));
    if (!reply.isOk())
      throw reply.getException();
    shadowConsumerCreated = true;
  }

  void addMessageEntry(MessageEntry messageEntry)
  {
    sessionQueue.stopQueue();
    if (messageChunk == null)
      messageChunk = new RingBuffer(32);
    messageChunk.add(messageEntry);
  }

  void assignLastMessage() throws Exception
  {
    if (lastMessage != null)
    {
      Reply reply = requestRegistry.request(new AssociateMessageRequest(dispatchId, lastMessage.getMessageIndex()));
      if (!reply.isOk())
        throw reply.getException();
    }
  }

  void setAutoAssign(boolean autoAssign)
  {
    this.autoAssign = autoAssign;
  }

  public boolean acknowledgeMessage(MessageIndex messageIndex) throws JMSException
  {
    if (closed)
      throw new javax.jms.IllegalStateException("Connection is closed");
    Reply reply = requestRegistry.request(new AcknowledgeMessageRequest(dispatchId, 0, messageIndex));
    if (!reply.isOk())
      throw ExceptionConverter.convert(reply.getException());
    return false;
  }

  public synchronized void run()
  {
    if (closed)
      return;
    if (messageListener == null)
      throw new RuntimeException("No MessageListener has been set!");
    if (messageChunk != null)
    {
      while (messageChunk.getSize() > 0)
      {
        try
        {
          lastMessage = (MessageEntry) messageChunk.remove();
          lastMessage.moveMessageAttributes();
          // eventually assign message to session
          if (autoAssign)
            assignLastMessage();
          lastMessage.getMessage().setSessionImpl(this);
          lastMessage.getMessage().setReadOnly(true);
          lastMessage.getMessage().reset();
          lastMessage.getMessage().setUseThreadContextCL(useThreadContextCL);
          messageListener.onMessage(lastMessage.getMessage());
          // eventually ack message when auto-ack
          if (!transacted && acknowledgeMode != Session.CLIENT_ACKNOWLEDGE)
            acknowledgeMessage(lastMessage.getMessageIndex());
        } catch (Exception e)
        {
          throw new RuntimeException(e.toString());
        }
        lastMessage = null;
      }
    }
  }

  private synchronized void doDeliverMessage(AsyncMessageDeliveryRequest request)
  {
    if (closed || recoveryInProgress || request.getRecoveryEpoche() < recoveryEpoche)
      return;
    int consumerId = request.getListenerId();

    MessageConsumerImpl consumer = (MessageConsumerImpl) consumerMap.get(new Integer(consumerId));
    if (consumer != null)
    {
      if (request.isBulk())
        consumer.addToCache(request.createRequests(), request.isRequiresRestart());
      else
        consumer.addToCache(request);
    }
  }

  void triggerInvocation()
  {
    sessionQueue.triggerInvocation();
  }

  public void serviceRequest(Request request)
  {
    sessionQueue.enqueue(request);
  }

  private class SessionDeliveryQueue extends SingleProcessorQueue
  {
    Object dummy = new Object();

    public SessionDeliveryQueue()
    {
      super(100);
    }

    protected void startProcessor()
    {
      if (!closed)
        sessionPool.dispatchTask(sessionTask);
    }

    void triggerInvocation()
    {
      enqueue(dummy);
    }

    protected void process(Object[] bulk, int n)
    {
      for (int i = 0; i < n; i++)
      {
        if (bulk[i] != dummy)
          doDeliverMessage((AsyncMessageDeliveryRequest) bulk[i]);
      }
      boolean moreToDeliver = false;
      synchronized (SessionImpl.this)
      {
        if (recoveryInProgress)
          return;
        for (Iterator iter = consumerMap.entrySet().iterator(); iter.hasNext();)
        {
          MessageConsumerImpl c = (MessageConsumerImpl) ((Map.Entry) iter.next()).getValue();
          boolean b = c.invokeConsumer();
          if (b)
            moreToDeliver = true;
        }
      }
      if (moreToDeliver)
        enqueue(dummy);
    }
  }

  private class SessionTask implements AsyncTask
  {
    public boolean isValid()
    {
      return !closed;
    }

    public String getDispatchToken()
    {
      return DISPATCH_TOKEN;
    }

    public String getDescription()
    {
      return myConnection.myHostname + "/Session/SessionTask";
    }

    public void run()
    {
      if (!closed && sessionQueue.dequeue())
        sessionPool.dispatchTask(this);
    }

    public void stop()
    {
    }
  }
}



