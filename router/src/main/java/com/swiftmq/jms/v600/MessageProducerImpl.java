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

package com.swiftmq.jms.v600;

import com.swiftmq.jms.*;
import com.swiftmq.jms.smqp.v600.CloseProducerRequest;
import com.swiftmq.jms.smqp.v600.ProduceMessageReply;
import com.swiftmq.jms.smqp.v600.ProduceMessageRequest;
import com.swiftmq.jms.smqp.v600.SMQPUtil;
import com.swiftmq.tools.requestreply.*;
import com.swiftmq.tools.tracking.MessageTracker;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.util.IdGenerator;

import javax.jms.*;

public class MessageProducerImpl implements MessageProducer, RequestRetryValidator
{
  private static final boolean ASYNC_SEND = Boolean.valueOf(System.getProperty("swiftmq.jms.persistent.asyncsend","false")).booleanValue();
  boolean closed = false;
  public volatile int producerId = -1;
  RequestRegistry requestRegistry = null;
  String myHostname = null;
  SessionImpl mySession = null;
  boolean disableMessageId = false;
  boolean disableTimestamp = false;
  int deliveryMode;
  int priority;
  long timeToLive;
  int tsInc = 0;
  int nSend = 0;
  long currentDelay = 0;
  int replyThreshold = 0;
  String uniqueProducerId = IdGenerator.getInstance().nextId('/');
  // JMS 1.1
  DestinationImpl destImpl = null;
  String clientId = null;
  DataByteArrayOutputStream dbos = new DataByteArrayOutputStream(2048);

  public MessageProducerImpl(SessionImpl mySession, int producerId,
                             RequestRegistry requestRegistry,
                             String myHostname, String clientId)
  {
    this.mySession = mySession;
    this.producerId = producerId;
    this.requestRegistry = requestRegistry;
    this.myHostname = myHostname;
    this.clientId = clientId;
    replyThreshold = mySession.getMyConnection().getSmqpProducerReplyInterval();
    disableMessageId = !mySession.getMyConnection().isJmsMessageIdEnabled();
    disableTimestamp = !mySession.getMyConnection().isJmsMessageTimestampEnabled();
    deliveryMode = mySession.getMyConnection().getJmsDeliveryMode();
    priority = mySession.getMyConnection().getJmsPriority();
    timeToLive = mySession.getMyConnection().getJmsTTL();
  }

  public void validate(Request request) throws ValidationException
  {
    if (request instanceof ProduceMessageRequest)
    {
      try
      {
        ProduceMessageRequest r = (ProduceMessageRequest) request;
        r.setDispatchId(mySession.dispatchId);
        r.setQueueProducerId(producerId);
        MessageImpl msg = SMQPUtil.getMessage(r);
        Destination dest = msg.getJMSDestination();
        // Messages for temp queues/topics needs to be cancelled because
        // the temp destination might be assigned to another app after
        // failover
        if (dest instanceof TemporaryQueue || dest instanceof TemporaryTopic)
        {
          r.setCancelledByValidator(true);
          return;
        }
        msg.setBooleanProperty(MessageImpl.PROP_DOUBT_DUPLICATE, true);
        if (r.getSingleMessage() != null)
          r.setSingleMessage(msg);
        else
          r.setMessageCopy(SMQPUtil.toBytes(msg));
      } catch (Exception e)
      {
        e.printStackTrace();
        throw new ValidationException(e.toString());
      }
    } else
    {
      CloseProducerRequest r = (CloseProducerRequest) request;
      r.setDispatchId(mySession.dispatchId);
      r.setQueueProducerId(producerId);
    }
  }

  protected void verifyState() throws JMSException
  {
    if (closed)
    {
      throw new JMSException("Message producer is closed");
    }

    mySession.verifyState();
  }

  private String nextId()
  {
    StringBuffer b = new StringBuffer(myHostname);
    b.append('/');
    b.append(uniqueProducerId);
    b.append('/');
    b.append(tsInc++);
    if (tsInc == Integer.MAX_VALUE)
      tsInc = 0;
    return b.toString();
  }

  Message initMessageForSend(Message message) throws JMSException
  {
    MessageImpl msg = null;
    if (mySession.withinOnMessage || !(message instanceof com.swiftmq.jms.MessageImpl))
      msg = (MessageImpl) MessageCloner.cloneMessage(message);
    else
      msg = (MessageImpl) message;
    msg.clearSwiftMQAllProps();
    msg.setReadOnly(false);
    msg.removeProperty(MessageImpl.PROP_DOUBT_DUPLICATE);
    msg.setStringProperty(MessageImpl.PROP_USER_ID, mySession.getUserName());
    msg.setJMSDeliveryMode(deliveryMode);
    msg.setJMSPriority(priority);
    msg.setJMSExpiration(timeToLive == 0 ? timeToLive : System.currentTimeMillis() + timeToLive);

    if (!disableTimestamp)
    {
      msg.setJMSTimestamp(System.currentTimeMillis());
    }

    if (!disableMessageId)
    {
      msg.setJMSMessageID(nextId());
    }
    return msg;
  }

  void processSend(int producerId, Message message) throws JMSException
  {
    boolean transacted = mySession.getTransacted();
    MessageImpl msg = (MessageImpl) message;

    if (transacted)
    {
      if (MessageTracker.enabled)
      {
        MessageTracker.getInstance().track((MessageImpl) msg, new String[]{mySession.myConnection.toString(), mySession.toString(), toString()}, "processSend, storeTransactedMessage");
      }
      mySession.storeTransactedMessage(this, msg);
    } else
    {
      nSend++;
      ProduceMessageReply reply = null;
      boolean replyRequired = nSend == replyThreshold || msg.getJMSDeliveryMode() == DeliveryMode.PERSISTENT && !ASYNC_SEND;
      try
      {
        ProduceMessageRequest request = null;
        if (!replyRequired)
        {
          dbos.rewind();
          msg.writeContent(dbos);
          byte[] b = new byte[dbos.getCount()];
          System.arraycopy(dbos.getBuffer(), 0, b, 0, b.length);
          request = new ProduceMessageRequest(this, mySession.dispatchId, producerId, null, b);
        } else
          request = new ProduceMessageRequest(this, mySession.dispatchId, producerId, msg, null);
        request.setReplyRequired(replyRequired);
        if (MessageTracker.enabled)
        {
          MessageTracker.getInstance().track((MessageImpl) msg, new String[]{mySession.myConnection.toString(), mySession.toString(), toString()}, "processSend ...");
        }
        reply = (ProduceMessageReply) requestRegistry.request(request);
        if (MessageTracker.enabled)
        {
          MessageTracker.getInstance().track((MessageImpl) msg, new String[]{mySession.myConnection.toString(), mySession.toString(), toString()}, "processSend done, reply=" + reply);
        }
      } catch (Exception e)
      {
        if (MessageTracker.enabled)
        {
          MessageTracker.getInstance().track((MessageImpl) msg, new String[]{mySession.myConnection.toString(), mySession.toString(), toString()}, "processSend, exception=" + e);
        }
        e.printStackTrace();
        throw ExceptionConverter.convert(e);
      }

      if (replyRequired)
      {
        if (reply == null)
          throw new JMSException("Request was cancelled (reply == null)");
        nSend = 0;
        if (!reply.isOk())
        {
          throw ExceptionConverter.convert(reply.getException());
        }
        currentDelay = reply.getDelay();
        if (currentDelay > 0)
        {
          try
          {
            Thread.sleep(currentDelay);
          } catch (Exception ignored)
          {
          }
        }
      }
    }
    // fix 1.2
    msg.reset();
  }

  public void setDestinationImpl(Destination destImpl)
  {
    this.destImpl = (DestinationImpl) destImpl;
  }

  // --> JMS 1.1
  public Destination getDestination() throws JMSException
  {
    verifyState();
    return destImpl;
  }

  private boolean isTopicDestination()
  {
    return destImpl.getType() == DestinationFactory.TYPE_TEMPTOPIC ||
        destImpl.getType() == DestinationFactory.TYPE_TOPIC;
  }

  private boolean isTopicDestination(DestinationImpl dest)
  {
    return dest.getType() == DestinationFactory.TYPE_TEMPTOPIC ||
        dest.getType() == DestinationFactory.TYPE_TOPIC;
  }

  public void send(Message message) throws JMSException
  {
    verifyState();

    if (this.destImpl == null)
      throw new UnsupportedOperationException("Cannot send unidentified on an unidentified MessageProducer!");

    Message msg = initMessageForSend(message);
    msg.setJMSDestination(destImpl);
    if (isTopicDestination() && clientId != null)
      msg.setStringProperty(MessageImpl.PROP_CLIENT_ID, clientId);
    processSend(producerId, msg);
  }

  public void send(Message message, int deliveryMode, int priority, long ttl) throws JMSException
  {
    verifyState();

    if (this.destImpl == null)
      throw new UnsupportedOperationException("Cannot send unidentified on an unidentified MessageProducer!");

    Message msg = initMessageForSend(message);
    msg.setJMSDeliveryMode(deliveryMode);
    msg.setJMSPriority(priority);
    msg.setJMSExpiration(ttl == 0 ? ttl : System.currentTimeMillis() + ttl);
    msg.setJMSDestination(destImpl);
    if (isTopicDestination() && clientId != null)
      msg.setStringProperty(MessageImpl.PROP_CLIENT_ID, clientId);
    processSend(producerId, msg);
  }

  public void send(Destination dest, Message message) throws JMSException
  {
    verifyState();

    if (this.destImpl != null)
      throw new UnsupportedOperationException("This send method is only supported for unidentified MessageProducer!");

    Message msg = initMessageForSend(message);
    msg.setJMSDestination(dest);
    if (isTopicDestination((DestinationImpl) dest) && clientId != null)
      msg.setStringProperty(MessageImpl.PROP_CLIENT_ID, clientId);
    processSend(-1, msg);
  }

  public void send(Destination dest, Message message, int deliveryMode, int priority, long ttl) throws JMSException
  {
    verifyState();

    if (this.destImpl != null)
      throw new UnsupportedOperationException("This send method is only supported for unidentified MessageProducer!");

    Message msg = initMessageForSend(message);
    msg.setJMSDeliveryMode(deliveryMode);
    msg.setJMSPriority(priority);
    msg.setJMSExpiration(ttl == 0 ? ttl
        : System.currentTimeMillis() + ttl);
    msg.setJMSDestination(dest);
    if (isTopicDestination((DestinationImpl) dest) && clientId != null)
      msg.setStringProperty(MessageImpl.PROP_CLIENT_ID, clientId);
    processSend(-1, msg);
  }
  // <-- JMS 1.1

  /**
   * Set whether message IDs are disabled.
   * <p/>
   * <P>Since message ID's take some effort to create and increase a
   * message's size, some JMS providers may be able to optimize message
   * overhead if they are given a hint that message ID is not used by
   * an application. JMS message Producers provide a hint to disable
   * message ID. When a client sets a Producer to disable message ID
   * they are saying that they do not depend on the value of message
   * ID for the messages it produces. These messages must either have
   * message ID set to null or, if the hint is ignored, messageID must
   * be set to its normal unique value.
   * <p/>
   * <P>Message IDs are enabled by default.
   *
   * @param value indicates if message IDs are disabled.
   */
  public void setDisableMessageID(boolean value) throws JMSException
  {
    verifyState();

    disableMessageId = value;
  }

  /**
   * Get an indication of whether message IDs are disabled.
   *
   * @return an indication of whether message IDs are disabled.
   */
  public boolean getDisableMessageID() throws JMSException
  {
    verifyState();

    return (disableMessageId);
  }

  /**
   * Set whether message timestamps are disabled.
   * <p/>
   * <P>Since timestamps take some effort to create and increase a
   * message's size, some JMS providers may be able to optimize message
   * overhead if they are given a hint that timestamp is not used by an
   * application. JMS message Producers provide a hint to disable
   * timestamps. When a client sets a producer to disable timestamps
   * they are saying that they do not depend on the value of timestamp
   * for the messages it produces. These messages must either have
   * timestamp set to null or, if the hint is ignored, timestamp must
   * be set to its normal value.
   * <p/>
   * <P>Message timestamps are enabled by default.
   *
   * @param value indicates if message timestamps are disabled.
   */
  public void setDisableMessageTimestamp(boolean value) throws JMSException
  {
    verifyState();

    disableTimestamp = value;
  }

  /**
   * Get an indication of whether message timestamps are disabled.
   *
   * @return an indication of whether message IDs are disabled.
   */
  public boolean getDisableMessageTimestamp() throws JMSException
  {
    verifyState();

    return (disableTimestamp);
  }

  /**
   * Set the producer's default delivery mode.
   * <p/>
   * <P>Delivery mode is set to PERSISTENT by default.
   *
   * @param dm the message delivery mode for this message
   *           producer.
   * @see javax.jms.MessageProducer#getDeliveryMode
   */
  public void setDeliveryMode(int dm) throws JMSException
  {
    verifyState();

    if (dm != DeliveryMode.PERSISTENT && dm != DeliveryMode.NON_PERSISTENT)
    {
      throw new JMSException("invalid delivery mode");
    }

    deliveryMode = dm;
  }

  /**
   * Get the producer's default delivery mode.
   *
   * @return the message delivery mode for this message producer.
   * @see javax.jms.MessageProducer#setDeliveryMode
   */
  public int getDeliveryMode() throws JMSException
  {
    verifyState();

    return (deliveryMode);
  }

  /**
   * Set the producer's default priority.
   * <p/>
   * <P>Priority is set to 4, by default.
   *
   * @see javax.jms.MessageProducer#getPriority
   */
  public void setPriority(int prio) throws JMSException
  {
    verifyState();

    if (prio < MessageImpl.MIN_PRIORITY || prio > MessageImpl.MAX_PRIORITY)
    {
      throw new JMSException("invalid priority, valid range is "
          + MessageImpl.MIN_PRIORITY + ".."
          + MessageImpl.MAX_PRIORITY);
    }

    priority = prio;
  }

  /**
   * Get the producer's default priority.
   *
   * @return the message priority for this message producer.
   * @see javax.jms.MessageProducer#setPriority
   */
  public int getPriority() throws JMSException
  {
    verifyState();

    return (priority);
  }

  /**
   * Set the default length of time in milliseconds from its dispatch time
   * that a produced message should be retained by the message system.
   * <p/>
   * <P>Time to live is set to zero by default.
   *
   * @param ttl the message time to live in milliseconds; zero is
   *            unlimited
   * @see javax.jms.MessageProducer#getTimeToLive
   */
  public void setTimeToLive(long ttl) throws JMSException
  {
    verifyState();

    if (ttl < 0)
    {
      throw new JMSException("invalid time to live");
    }

    timeToLive = ttl;
  }

  /**
   * Get the default length of time in milliseconds from its dispatch time
   * that a produced message should be retained by the message system.
   *
   * @return the message time to live in milliseconds; zero is unlimited
   * @see javax.jms.MessageProducer#setTimeToLive
   */
  public long getTimeToLive() throws JMSException
  {
    verifyState();

    return (timeToLive);
  }

  void _close(boolean delayClose) throws JMSException
  {
    if (closed || mySession.isClosed())
      return;
    if (delayClose)
    {
      if (mySession.transacted)
      {
        mySession.delayClose(this);
        return;
      }
    }

    closed = true;

    // Fix: 2.1.0
    if (producerId == -1)
      return;
    Reply reply = null;

    try
    {
      reply = requestRegistry.request(new CloseProducerRequest(this, mySession.dispatchId, producerId));
    } catch (Exception e)
    {
      throw ExceptionConverter.convert(e);
    }

    if (!reply.isOk())
    {
      throw ExceptionConverter.convert(reply.getException());
    }
    mySession.removeMessageProducerImpl(this);
  }

  /**
   * Since a provider may allocate some resources on behalf of a
   * MessageProducer outside the JVM, clients should close them when they
   * are not needed. Relying on garbage collection to eventually reclaim
   * these resources may not be timely enough.
   */
  public void close() throws JMSException
  {
    _close(true);
  }

}



