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

package com.swiftmq.jms;

import com.swiftmq.swiftlet.queue.MessageIndex;
import com.swiftmq.tools.tracking.MessageTracker;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.util.LazyUTF8String;
import com.swiftmq.tools.util.LengthCaptureDataInput;

import javax.jms.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Enumeration;

/**
 * Implementation of a Message.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2005, All Rights Reserved
 */
public class MessageImpl implements Message, Serializable
{
  public static final String PROP_DELIVERY_COUNT = "JMSXDeliveryCount";
  public static final String PROP_USER_ID = "JMSXUserID";
  public static final String PROP_CLIENT_ID = "JMS_SWIFTMQ_CID";
  public static final String PROP_UNROUTABLE_REASON = "JMS_SWIFTMQ_UR_REASON";
  public static final String PROP_DOUBT_DUPLICATE = "JMS_SWIFTMQ_DOUBT_DUPLICATE";
  public static final int MAX_PRIORITY = 9;
  public static final int MIN_PRIORITY = 1;
  public static final int DEFAULT_PRIORITY = 4;
  static final int MIN_COMPRESSION_SIZE = 2048;
  static final int TYPE_MESSAGE = 0;
  static final int TYPE_BYTESMESSAGE = 1;
  static final int TYPE_MAPMESSAGE = 2;
  static final int TYPE_OBJECTMESSAGE = 3;
  static final int TYPE_STREAMMESSAGE = 4;
  static final int TYPE_TEXTMESSAGE = 5;
  boolean readOnly = false;
  LazyUTF8String messageId = null;
  long timeStamp = 0;
  LazyUTF8String correlationId = null;
  Destination replyTo = null;
  Destination destination = null;
  int deliveryMode = DeliveryMode.NON_PERSISTENT;
  boolean redelivered = false;
  LazyUTF8String type = null;
  MessageIndex messageIndex = null;
  long expiration = 0;
  int priority = 0;
  int deliveryCount = 0;
  MessageProperties props = null;
  byte[] propBytes = null;
  LazyUTF8String userId = null;
  LazyUTF8String clientId = null;
  transient SwiftMQMessageConsumer myConsumer = null;
  transient SwiftMQSession mySession = null;
  transient volatile boolean cancelled = false;
  transient volatile String duplicateId = null;
  transient long messageLength = -1;
    transient volatile Object persistentKey = null;
    transient volatile Object streamPKey = null;

  // Routing
  LazyUTF8String sourceRouter = null;
  LazyUTF8String destRouter = null;
  LazyUTF8String destQueue = null;

  protected int getType()
  {
    return TYPE_MESSAGE;
  }

  public static MessageImpl createInstance(int type)
  {
    MessageImpl msg = null;

    switch (type)
    {

      case TYPE_MESSAGE:
        msg = new MessageImpl();
        break;

      case TYPE_BYTESMESSAGE:
        msg = new BytesMessageImpl();
        break;

      case TYPE_MAPMESSAGE:
        msg = new MapMessageImpl();
        break;

      case TYPE_OBJECTMESSAGE:
        msg = new ObjectMessageImpl();
        break;

      case TYPE_STREAMMESSAGE:
        msg = new StreamMessageImpl();
        break;

      case TYPE_TEXTMESSAGE:
        msg = new TextMessageImpl();
        break;
    }

    return msg;
  }

  public Object getPersistentKey()
  {
    return persistentKey;
  }

  public void setPersistentKey(Object persistentKey)
  {
    this.persistentKey = persistentKey;
  }

    public Object getStreamPKey() {
        return streamPKey;
    }

    public void setStreamPKey(Object streamPKey) {
        this.streamPKey = streamPKey;
    }

    public long getMessageLength()
  {
    return messageLength;
  }

  public boolean isCancelled()
  {
    return cancelled;
  }

  public String getDuplicateId()
  {
    return duplicateId;
  }

  public void setDuplicateId(String duplicateId)
  {
    this.duplicateId = duplicateId;
  }

  private void writeHeader(DataOutput out) throws IOException
  {
    out.writeLong(expiration);
    out.writeInt(priority);
    out.writeLong(timeStamp);
    out.writeInt(deliveryCount);
    out.writeInt(deliveryMode);
    out.writeBoolean(redelivered);

    if (messageId == null)
      out.writeByte(0);
    else
    {
      out.writeByte(1);
      messageId.writeContent(out);
    }

    if (userId == null)
      out.writeByte(0);
    else
    {
      out.writeByte(1);
      userId.writeContent(out);
    }

    if (clientId == null)
      out.writeByte(0);
    else
    {
      out.writeByte(1);
      clientId.writeContent(out);
    }

    if (correlationId == null)
      out.writeByte(0);
    else
    {
      out.writeByte(1);
      correlationId.writeContent(out);
    }

    if (type == null)
      out.writeByte(0);
    else
    {
      out.writeByte(1);
      type.writeContent(out);
    }

    if (replyTo == null)
      out.writeByte(0);
    else
    {
      out.writeByte(1);
      DestinationFactory.dumpDestination((DestinationImpl) replyTo, out);
    }

    if (destination == null)
      out.writeByte(0);
    else
    {
      out.writeByte(1);
      DestinationFactory.dumpDestination((DestinationImpl) destination, out);
    }

  }

  private void writeProperties(DataOutput out) throws IOException
  {
    if (propBytes != null)
    {
      out.writeByte(1);
      out.writeInt(propBytes.length);
      out.write(propBytes, 0, propBytes.length);
    } else if (props == null)
    {
      out.writeByte(0);
    } else
    {
      out.writeByte(1);
      DataByteArrayOutputStream dos = new DataByteArrayOutputStream(128);
      props.writeContent(dos);
      dos.close();
      out.writeInt(dos.getCount());
      out.write(dos.getBuffer(), 0, dos.getCount());
    }
  }

  private void writeRouting(DataOutput out) throws IOException
  {
    if (sourceRouter == null)
    {
      out.writeByte(0);
    } else
    {
      out.writeByte(1);
      sourceRouter.writeContent(out);
    }

    if (destRouter == null)
    {
      out.writeByte(0);
    } else
    {
      out.writeByte(1);
      destRouter.writeContent(out);
    }

    if (destQueue == null)
    {
      out.writeByte(0);
    } else
    {
      out.writeByte(1);
      destQueue.writeContent(out);
    }
  }

  private void writeEmptyRouting(DataOutput out) throws IOException
  {
    out.writeByte(0);
    out.writeByte(0);
    out.writeByte(0);
  }

  protected void writeBody(DataOutput out) throws IOException
  {
  }

  public void writeContent(DataOutput out) throws IOException
  {
    out.writeInt(getType());
    writeHeader(out);
    writeProperties(out);
    writeRouting(out);
    writeBody(out);
  }

  // Writes without routing headers (performance)
  public void writeContent(ToClientSerializer serializer) throws IOException
  {
    DataOutput out = serializer.getDataOutput();
    out.writeInt(getType());
    writeHeader(out);
    writeProperties(out);
    writeEmptyRouting(out);
    writeBody(out);
  }

  private void readHeader(DataInput in) throws IOException
  {
    expiration = in.readLong();
    priority = in.readInt();
    timeStamp = in.readLong();
    deliveryCount = in.readInt();
    deliveryMode = in.readInt();
    redelivered = in.readBoolean();

    byte set = in.readByte();
    if (set == 1)
      messageId = new LazyUTF8String(in);
    else
      messageId = null;

    set = in.readByte();
    if (set == 1)
      userId = new LazyUTF8String(in);
    else
      userId = null;

    set = in.readByte();
    if (set == 1)
      clientId = new LazyUTF8String(in);
    else
      clientId = null;

    set = in.readByte();
    if (set == 1)
      correlationId = new LazyUTF8String(in);
    else
      correlationId = null;

    set = in.readByte();
    if (set == 1)
      type = new LazyUTF8String(in);
    else
      type = null;

    set = in.readByte();
    if (set == 1)
      replyTo = DestinationFactory.createDestination(in);
    else
      replyTo = null;

    set = in.readByte();
    if (set == 1)
      destination = DestinationFactory.createDestination(in);
    else
      destination = null;
  }

  private void readProperties(DataInput in) throws IOException
  {
    byte set = in.readByte();
    if (set == 0)
    {
      props = null;
      propBytes = null;
    } else
    {
      propBytes = new byte[in.readInt()];
      in.readFully(propBytes);
    }
  }

  private void readRouting(DataInput in) throws IOException
  {
    byte set = in.readByte();
    if (set == 1)
      sourceRouter = new LazyUTF8String(in);
    else
      sourceRouter = null;

    set = in.readByte();
    if (set == 1)
      destRouter = new LazyUTF8String(in);
    else
      destRouter = null;

    set = in.readByte();
    if (set == 1)
      destQueue = new LazyUTF8String(in);
    else
      destQueue = null;
  }

  protected void readBody(DataInput in) throws IOException
  {
  }

  public void readContent(LengthCaptureDataInput in) throws IOException
  {
    in.startCaptureLength();
    readHeader(in);
    readProperties(in);
    readRouting(in);
    readBody(in);
    messageLength = in.stopCaptureLength();
  }

  public void readContent(DataInput in) throws IOException
  {
    try
    {
      readContent((LengthCaptureDataInput) in);
    } catch (ClassCastException e)
    {
      readHeader(in);
      readProperties(in);
      readRouting(in);
      readBody(in);
    }
  }

  public void setUseThreadContextCL(boolean b)
  {
  }

  private void unfold(LazyUTF8String lazyUTF8String)
  {
    if (lazyUTF8String != null)
      lazyUTF8String.getString(true);
  }

  private void unfold(Destination destination)
  {
    if (destination != null)
      ((DestinationImpl) destination).unfoldBuffers();
  }

  public void unfoldBuffers()
  {
    unfold(messageId);
    unfold(correlationId);
    unfold(type);
    unfold(userId);
    unfold(clientId);
    unfold(destQueue);
    unfold(destRouter);
    unfold(sourceRouter);
    unfold(destination);
    unfold(replyTo);
    checkProps();
    propBytes = null;
    unfoldBody();
  }

  protected void unfoldBody()
  {
  }

  // Internal use for selectors without exception handling
  public Object getField(String name)
  {
    Object obj = null;
    try
    {
      if (name.equals("JMSMessageID"))
        obj = messageId != null ? messageId.getString() : null;
      else if (name.equals("JMSPriority"))
        obj = new Integer(priority);
      else if (name.equals("JMSTimestamp"))
        obj = new Long(timeStamp);
      else if (name.equals("JMSCorrelationID"))
        obj = correlationId != null ? correlationId.getString() : null;
      else if (name.equals("JMSDeliveryMode"))
        obj = deliveryMode == DeliveryMode.PERSISTENT ? "PERSISTENT" : "NON_PERSISTENT";
      else if (name.equals("JMSType"))
        obj = type != null ? type.getString() : null;
      else if (name.equals("JMSDestination"))
        obj = destination != null ? destination.toString() : null;
      else
        obj = getObjectProperty(name);

      if (obj != null)
      {
        if (obj instanceof Number)
          obj = new Double(((Number) obj).doubleValue());
      }
    } catch (JMSException e)
    {
    }
    return obj;
  }

  public void clearSwiftMQProps()
  {
  }

  public void clearSwiftMQAllProps()
  {
    sourceRouter = null;
    destRouter = null;
    messageIndex = null;
    userId = null;
    clientId = null;
    deliveryCount = 0;
  }

  private void checkProps()
  {
    if (props == null)
    {
      props = new MessageProperties();
      if (propBytes != null)
      {
        try
        {
          DataByteArrayInputStream dis = new DataByteArrayInputStream(propBytes);
          props.readContent(dis);
        } catch (IOException e)
        {
          e.printStackTrace();
        }
      }
    }
  }

  private void verifyName(String name) throws JMSException
  {
    if (name != null && name.length() > 0)
    {
      for (int i = 0; i < name.length(); i++)
      {
        char c = name.charAt(i);
        if (i == 0)
        {
          if (!Character.isJavaIdentifierStart(c))
            throw new JMSException("'" + name + "' doesn't start with a valid Java Identifier Start Character!");
        } else
        {
          if (!Character.isJavaIdentifierPart(c))
            throw new JMSException("'" + name + "' includes an invalid character!");
        }
      }
    }
  }

  public void setMessageConsumerImpl(SwiftMQMessageConsumer myConsumer)
  {
    this.myConsumer = myConsumer;
  }

  public void setSessionImpl(SwiftMQSession mySession)
  {
    this.mySession = mySession;
  }

  public void setReadOnly(boolean b)
  {
    readOnly = b;
  }

  public void setMessageIndex(MessageIndex messageIndex)
  {
    this.messageIndex = messageIndex;
  }

  public MessageIndex getMessageIndex()
  {
    return messageIndex;
  }

  public void setSourceRouter(String sourceRouter)
  {
    this.sourceRouter = sourceRouter != null ? new LazyUTF8String(sourceRouter) : null;
  }

  public String getSourceRouter()
  {
    return sourceRouter != null ? sourceRouter.getString() : null;
  }

  public void setDestRouter(String destRouter)
  {
    this.destRouter = destRouter != null ? new LazyUTF8String(destRouter) : null;
  }

  public String getDestRouter()
  {
    return destRouter != null ? destRouter.getString() : null;
  }

  public void setDestQueue(String destQueue)
  {
    this.destQueue = destQueue != null ? new LazyUTF8String(destQueue) : null;
  }

  public String getDestQueue()
  {
    return destQueue != null ? destQueue.getString() : null;
  }

  public void removeProperty(String name)
  {
    if (props != null)
      props.remove(name);
  }

  /**
   * Get the message ID.
   * <p/>
   * <P>The messageID header field contains a value that uniquely
   * identifies each message sent by a provider.
   * <p/>
   * <P>When a message is sent, messageID can be ignored. When
   * the send method returns it contains a provider-assigned value.
   * <p/>
   * <P>A JMSMessageID is a String value which should function as a
   * unique key for identifying messages in a historical repository.
   * The exact scope of uniqueness is provider defined. It should at
   * least cover all messages for a specific installation of a
   * provider where an installation is some connected set of message
   * routers.
   * <p/>
   * <P>All JMSMessageID values must start with the prefix `ID:'.
   * Uniqueness of message ID values across different providers is
   * not required.
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
   *
   * @return the message ID
   * @throws JMSException if JMS fails to get the message Id
   *                      due to internal JMS error.
   */
  public String getJMSMessageID() throws JMSException
  {
    return messageId != null ? messageId.getString() : null;
  }

  /**
   * Set the message ID.
   * <p/>
   * <P>Providers set this field when a message is sent. This operation
   * can be used to change the value of a message that's been received.
   *
   * @param id the ID of the message
   * @throws JMSException if JMS fails to set the message Id
   *                      due to internal JMS error.
   * @see javax.jms.Message#getJMSMessageID()
   */
  public void setJMSMessageID(String id) throws JMSException
  {
    if (id != null)
    {
      StringBuffer b = new StringBuffer(id.length() + 5);
      b.append("ID:");
      b.append(id);
      messageId = new LazyUTF8String(b.toString());
    } else
      messageId = null;
  }

  /**
   * Get the message timestamp.
   * <p/>
   * <P>The JMSTimestamp header field contains the time a message was
   * handed off to a provider to be sent. It is not the time the
   * message was actually transmitted because the actual send may occur
   * later due to transactions or other client side queueing of messages.
   * <p/>
   * <P>When a message is sent, JMSTimestamp is ignored. When the send
   * method returns it contains a a time value somewhere in the interval
   * between the call and the return. It is in the format of a normal
   * Java millis time value.
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
   *
   * @return the message timestamp
   * @throws JMSException if JMS fails to get the Timestamp
   *                      due to internal JMS error.
   */
  public long getJMSTimestamp() throws JMSException
  {
    return (timeStamp);
  }

  /**
   * Set the message timestamp.
   * <p/>
   * <P>Providers set this field when a message is sent. This operation
   * can be used to change the value of a message that's been received.
   *
   * @param timestamp the timestamp for this message
   * @throws JMSException if JMS fails to set the timestamp
   *                      due to some internal JMS error.
   * @see javax.jms.Message#getJMSTimestamp()
   */
  public void setJMSTimestamp(long timestamp) throws JMSException
  {
    this.timeStamp = timestamp;
  }

  /**
   * Get the correlation ID as an array of bytes for the message.
   * <p/>
   * <P>The use of a byte[] value for JMSCorrelationID is non-portable.
   *
   * @return the correlation ID of a message as an array of bytes.
   * @throws JMSException if JMS fails to get correlationId
   *                      due to some internal JMS error.
   */
  public byte[] getJMSCorrelationIDAsBytes() throws JMSException
  {
    return (correlationId == null ? null : correlationId.getString().getBytes());
  }

  /**
   * Set the correlation ID as an array of bytes for the message.
   * <p/>
   * <P>If a provider supports the native concept of correlation id, a
   * JMS client may need to assign specific JMSCorrelationID values to
   * match those expected by non-JMS clients. JMS providers without native
   * correlation id values are not required to support this (and the
   * corresponding get) method; their implementation may throw
   * java.lang.UnsupportedOperationException).
   * <p/>
   * <P>The use of a byte[] value for JMSCorrelationID is non-portable.
   *
   * @param correlationID the correlation ID value as an array of bytes.
   * @throws JMSException if JMS fails to set correlationId
   *                      due to some internal JMS error.
   */
  public void setJMSCorrelationIDAsBytes(byte[] correlationID)
      throws JMSException
  {
    this.correlationId = correlationID != null ? new LazyUTF8String(new String(correlationID)) : null;
  }

  /**
   * Set the correlation ID for the message.
   * <p/>
   * <P>A client can use the JMSCorrelationID header field to link one
   * message with another. A typically use is to link a response message
   * with its request message.
   * <p/>
   * <P>JMSCorrelationID can hold one of the following:
   * <UL>
   * <LI>A provider-specific message ID
   * <LI>An application-specific String
   * <LI>A provider-native byte[] value.
   * </UL>
   * <p/>
   * <P>Since each message sent by a JMS provider is assigned a message ID
   * value it is convenient to link messages via message ID. All message ID
   * values must start with the `ID:' prefix.
   * <p/>
   * <P>In some cases, an application (made up of several clients) needs to
   * use an application specific value for linking messages. For instance,
   * an application may use JMSCorrelationID to hold a value referencing
   * some external information. Application specified values must not start
   * with the `ID:' prefix; this is reserved for provider-generated message
   * ID values.
   * <p/>
   * <P>If a provider supports the native concept of correlation ID, a JMS
   * client may need to assign specific JMSCorrelationID values to match
   * those expected by non-JMS clients. A byte[] value is used for this
   * purpose. JMS providers without native correlation ID values are not
   * required to support byte[] values. The use of a byte[] value for
   * JMSCorrelationID is non-portable.
   *
   * @param correlationID the message ID of a message being referred to.
   * @throws JMSException if JMS fails to set correlationId
   *                      due to some internal JMS error.
   */
  public void setJMSCorrelationID(String correlationID) throws JMSException
  {
    this.correlationId = correlationID != null ? new LazyUTF8String(correlationID) : null;
  }

  /**
   * Get the correlation ID for the message.
   * <p/>
   * <P>This method is used to return correlation id values that are
   * either provider-specific message ID's or application-specific Strings.
   *
   * @return the correlation ID of a message as a String.
   * @throws JMSException if JMS fails to get correlationId
   *                      due to some internal JMS error.
   */
  public String getJMSCorrelationID() throws JMSException
  {
    return correlationId != null ? correlationId.getString() : null;
  }

  /**
   * Get where a reply to this message should be sent.
   *
   * @return where to send a response to this message
   * @throws JMSException if JMS fails to get ReplyTo Destination
   *                      due to some internal JMS error.
   */
  public Destination getJMSReplyTo() throws JMSException
  {
    return (replyTo);
  }

  /**
   * Set where a reply to this message should be sent.
   * <p/>
   * <P>The replyTo header field contains the destination where a reply
   * to the current message should be sent. If it is null no reply is
   * expected. The destination may be either a Queue or a Topic.
   * <p/>
   * <P>Messages with a null replyTo value are called JMS datagrams.
   * Datagrams may be a notification of some change in the sender (i.e.
   * they signal a sender event) or they may just be some data the sender
   * thinks is of interest.
   * <p/>
   * Messages with a replyTo value are typically expecting a response.
   * A response may be optional, it is up to the client to decide. These
   * messages are called JMS requests. A message sent in response to a
   * request is called a reply.
   * <p/>
   * In some cases a client may wish to match up a request it sent earlier
   * with a reply it has just received. This can be done using the
   * correlationID.
   *
   * @param replyTo where to send a response to this message
   * @throws JMSException if JMS fails to set ReplyTo Destination
   *                      due to some internal JMS error.
   * @see javax.jms.Message#getJMSReplyTo()
   */
  public void setJMSReplyTo(Destination replyTo) throws JMSException
  {
    this.replyTo = replyTo;
  }

  /**
   * Get the destination for this message.
   * <p/>
   * <P>The destination field contains the destination to which the
   * message is being sent.
   * <p/>
   * <P>When a message is sent this value is ignored. After completion
   * of the send method it holds the destination specified by the send.
   * <p/>
   * <P>When a message is received, its destination value must be
   * equivalent to the value assigned when it was sent.
   *
   * @return the destination of this message.
   * @throws JMSException if JMS fails to get JMS Destination
   *                      due to some internal JMS error.
   */
  public Destination getJMSDestination() throws JMSException
  {
    return (destination);
  }

  /**
   * Set the destination for this message.
   * <p/>
   * <P>Providers set this field when a message is sent. This operation
   * can be used to change the value of a message that's been received.
   *
   * @param destination the destination for this message.
   * @throws JMSException if JMS fails to set JMS Destination
   *                      due to some internal JMS error.
   * @see javax.jms.Message#getJMSDestination()
   */
  public void setJMSDestination(Destination destination) throws JMSException
  {
    if (destination == null)
    {
      throw new JMSException("Invalid destination - null!");
    }

    this.destination = destination;
    setDestQueue(((QueueImpl) destination).getQueueName());
  }

  /**
   * Get the delivery mode for this message.
   *
   * @return the delivery mode of this message.
   * @throws JMSException if JMS fails to get JMS DeliveryMode
   *                      due to some internal JMS error.
   * @see javax.jms.DeliveryMode
   */
  public int getJMSDeliveryMode() throws JMSException
  {
    return (deliveryMode);
  }

  /**
   * Set the delivery mode for this message.
   * <p/>
   * <P>Providers set this field when a message is sent. This operation
   * can be used to change the value of a message that's been received.
   *
   * @param deliveryMode the delivery mode for this message.
   * @throws JMSException if JMS fails to set JMS DeliveryMode
   *                      due to some internal JMS error.
   * @see javax.jms.Message#getJMSDeliveryMode()
   * @see javax.jms.DeliveryMode
   */
  public void setJMSDeliveryMode(int deliveryMode) throws JMSException
  {
    this.deliveryMode = deliveryMode;
  }

  /**
   * Get an indication of whether this message is being redelivered.
   * <p/>
   * <P>If a client receives a message with the redelivered indicator set,
   * it is likely, but not guaranteed, that this message was delivered to
   * the client earlier but the client did not acknowledge its receipt at
   * that earlier time.
   *
   * @return set to true if this message is being redelivered.
   * @throws JMSException if JMS fails to get JMS Redelivered flag
   *                      due to some internal JMS error.
   */
  public boolean getJMSRedelivered() throws JMSException
  {
    return (redelivered);
  }

  /**
   * Set to indicate whether this message is being redelivered.
   * <p/>
   * <P>This field is set at the time the message is delivered. This
   * operation can be used to change the value of a message that's
   * been received.
   *
   * @param redelivered an indication of whether this message is being
   *                    redelivered.
   * @throws JMSException if JMS fails to set JMS Redelivered flag
   *                      due to some internal JMS error.
   * @see javax.jms.Message#getJMSRedelivered()
   */
  public void setJMSRedelivered(boolean redelivered) throws JMSException
  {
    this.redelivered = redelivered;
  }

  /**
   * Get the message type.
   *
   * @return the message type
   * @throws JMSException if JMS fails to get JMS message type
   *                      due to some internal JMS error.
   */
  public String getJMSType() throws JMSException
  {
    return type != null ? type.getString() : null;
  }

  /**
   * Set the message type.
   * <p/>
   * <P>Some JMS providers use a message repository that contains the
   * definition of messages sent by applications. The type header field
   * contains the name of a message's definition.
   * <p/>
   * <P>JMS does not define a standard message definition repository nor
   * does it define a naming policy for the definitions it contains. JMS
   * clients should use symbolic values for type that can be configured
   * at installation time to the values defined in the current providers
   * message repository.
   * <p/>
   * <P>JMS clients should assign a value to type whether the application
   * makes use of it or not. This insures that it is properly set for
   * those providers that require it.
   *
   * @param type the class of message
   * @throws JMSException if JMS fails to set JMS message type
   *                      due to some internal JMS error.
   * @see javax.jms.Message#getJMSType()
   */
  public void setJMSType(String type) throws JMSException
  {
    this.type = type != null ? new LazyUTF8String(type) : null;
  }

  /**
   * Get the message's expiration value.
   * <p/>
   * <P>When a message is sent, expiration is left unassigned. After
   * completion of the send method, it holds the expiration time of the
   * message. This is the sum of the time-to-live value specified by the
   * client and the GMT at the time of the send.
   * <p/>
   * <P>If the time-to-live is specified as zero, expiration is set to
   * zero which indicates the message does not expire.
   * <p/>
   * <P>When a message's expiration time is reached, a provider should
   * discard it. JMS does not define any form of notification of message
   * expiration.
   * <p/>
   * <P>Clients should not receive messages that have expired; however,
   * JMS does not guarantee that this will not happen.
   *
   * @return the time the message expires. It is the sum of the
   *         time-to-live value specified by the client, and the GMT at the
   *         time of the send.
   * @throws JMSException if JMS fails to get JMS message expiration
   *                      due to some internal JMS error.
   */
  public long getJMSExpiration() throws JMSException
  {
    return (expiration);
  }

  /**
   * Set the message's expiration value.
   * <p/>
   * <P>Providers set this field when a message is sent. This operation
   * can be used to change the value of a message that's been received.
   *
   * @param expiration the message's expiration time
   * @throws JMSException if JMS fails to set JMS message expiration
   *                      due to some internal JMS error.
   * @see javax.jms.Message#getJMSExpiration()
   */
  public void setJMSExpiration(long expiration) throws JMSException
  {
    this.expiration = expiration;
  }

  /**
   * Get the message priority.
   * <p/>
   * <P>JMS defines a ten level priority value with 0 as the lowest
   * priority and 9 as the highest. In addition, clients should consider
   * priorities 0-4 as gradations of normal priority and priorities 5-9
   * as gradations of expedited priority.
   * <p/>
   * <P>JMS does not require that a provider strictly implement priority
   * ordering of messages; however, it should do its best to deliver
   * expedited messages ahead of normal messages.
   *
   * @return the default message priority
   * @throws JMSException if JMS fails to get JMS message priority
   *                      due to some internal JMS error.
   */
  public int getJMSPriority() throws JMSException
  {
    return (priority);
  }

  /**
   * Set the priority for this message.
   * <p/>
   * <P>Providers set this field when a message is sent. This operation
   * can be used to change the value of a message that's been received.
   *
   * @param priority the priority of this message
   * @throws JMSException if JMS fails to set JMS message priority
   *                      due to some internal JMS error.
   * @see javax.jms.Message#getJMSPriority()
   */
  public void setJMSPriority(int priority) throws JMSException
  {
    this.priority = priority;
  }

  /**
   * Clear a message's properties.
   *
   * @throws JMSException if JMS fails to clear JMS message
   *                      properties due to some internal JMS
   *                      error.
   */
  public void clearProperties() throws JMSException
  {
    props = null;
    propBytes = null;
    readOnly = false;
  }

  /**
   * Check if a property value exists.
   *
   * @param name the name of the property to test
   * @return true if the property does exist.
   * @throws JMSException if JMS fails to  check if property
   *                      exists due to some internal JMS
   *                      error.
   */
  public boolean propertyExists(String name) throws JMSException
  {
    checkProps();
    return props.exists(name);
  }

  /**
   * Return the boolean property value with the given name.
   *
   * @param name the name of the boolean property
   * @return the boolean property value with the given name.
   * @throws JMSException           if JMS fails to  get Property due to
   *                                some internal JMS error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  public boolean getBooleanProperty(String name) throws JMSException
  {
    checkProps();
    return props.getBoolean(name);
  }

  /**
   * Return the byte property value with the given name.
   *
   * @param name the name of the byte property
   * @return the byte property value with the given name.
   * @throws JMSException           if JMS fails to  get Property due to
   *                                some internal JMS error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  public byte getByteProperty(String name) throws JMSException
  {
    checkProps();
    return props.getByte(name);
  }

  /**
   * Return the short property value with the given name.
   *
   * @param name the name of the short property
   * @return the short property value with the given name.
   * @throws JMSException           if JMS fails to  get Property due to
   *                                some internal JMS error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  public short getShortProperty(String name) throws JMSException
  {
    checkProps();
    return props.getShort(name);
  }

  /**
   * Return the integer property value with the given name.
   *
   * @param name the name of the integer property
   * @return the integer property value with the given name.
   * @throws JMSException           if JMS fails to  get Property due to
   *                                some internal JMS error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  public int getIntProperty(String name) throws JMSException
  {
    if (name.equals(PROP_DELIVERY_COUNT))
      return deliveryCount;
    checkProps();
    return props.getInt(name);
  }

  /**
   * Return the long property value with the given name.
   *
   * @param name the name of the long property
   * @return the long property value with the given name.
   * @throws JMSException           if JMS fails to  get Property due to
   *                                some internal JMS error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  public long getLongProperty(String name) throws JMSException
  {
    if (name.equals(PROP_DELIVERY_COUNT))
      return deliveryCount;
    checkProps();
    return props.getLong(name);
  }

  /**
   * Return the float property value with the given name.
   *
   * @param name the name of the float property
   * @return the float property value with the given name.
   * @throws JMSException           if JMS fails to  get Property due to
   *                                some internal JMS error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  public float getFloatProperty(String name) throws JMSException
  {
    checkProps();
    return props.getFloat(name);
  }

  /**
   * Return the double property value with the given name.
   *
   * @param name the name of the double property
   * @return the double property value with the given name.
   * @throws JMSException           if JMS fails to  get Property due to
   *                                some internal JMS error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  public double getDoubleProperty(String name) throws JMSException
  {
    checkProps();
    return props.getDouble(name);
  }

  /**
   * Return the String property value with the given name.
   *
   * @param name the name of the String property
   * @return the String property value with the given name. If there
   *         is no property by this name, a null value is returned.
   * @throws JMSException           if JMS fails to  get Property due to
   *                                some internal JMS error.
   * @throws MessageFormatException if this type conversion is invalid.
   */
  public String getStringProperty(String name) throws JMSException
  {
    if (name.equals(PROP_USER_ID))
      return userId != null ? userId.getString() : null;
    if (name.equals(PROP_CLIENT_ID))
      return clientId != null ? clientId.getString() : null;
    checkProps();
    return props.getString(name);
  }

  /**
   * Return the Java object property value with the given name.
   * <p/>
   * <P>Note that this method can be used to return in objectified format,
   * an object that had been stored as a property in the Message with the
   * equivalent <CODE>setObject</CODE> method call, or it's equivalent
   * primitive set<type> method.
   *
   * @param name the name of the Java object property
   * @return the Java object property value with the given name, in
   *         objectified format (ie. if it set as an int, then a Integer is
   *         returned). If there is no property by this name, a null value
   *         is returned.
   * @throws JMSException if JMS fails to  get Property due to
   *                      some internal JMS error.
   */
  public Object getObjectProperty(String name) throws JMSException
  {
    if (name.equals(PROP_USER_ID))
      return userId != null ? userId.getString() : null;
    if (name.equals(PROP_CLIENT_ID))
      return clientId != null ? clientId.getString() : null;
    if (name.equals(PROP_DELIVERY_COUNT))
      return String.valueOf(deliveryCount);
    checkProps();
    return props.getObject(name);
  }

  /**
   * Return an Enumeration of all the property names.
   *
   * @return an enumeration of all the names of property values.
   * @throws JMSException if JMS fails to  get Property names due to
   *                      some internal JMS error.
   */
  public Enumeration getPropertyNames() throws JMSException
  {
    checkProps();
    return props.enumeration();
  }

  /**
   * Set a boolean property value with the given name, into the Message.
   *
   * @param name  the name of the boolean property
   * @param value the boolean property value to set in the Message.
   * @throws JMSException                 if JMS fails to  set Property due to
   *                                      some internal JMS error.
   * @throws MessageNotWriteableException if properties are read-only
   */
  public void setBooleanProperty(String name,
                                 boolean value) throws JMSException
  {
    if (readOnly)
    {
      throw new MessageNotWriteableException("Message properties are read only");
    }
    verifyName(name);
    checkProps();
    props.setBoolean(name, value);
    propBytes = null;
  }

  /**
   * Set a byte property value with the given name, into the Message.
   *
   * @param name  the name of the byte property
   * @param value the byte property value to set in the Message.
   * @throws JMSException                 if JMS fails to  set Property due to
   *                                      some internal JMS error.
   * @throws MessageNotWriteableException if properties are read-only
   */
  public void setByteProperty(String name, byte value) throws JMSException
  {
    if (readOnly)
    {
      throw new MessageNotWriteableException("Message properties are read only");
    }
    verifyName(name);
    checkProps();
    props.setByte(name, value);
    propBytes = null;
  }

  /**
   * Set a short property value with the given name, into the Message.
   *
   * @param name  the name of the short property
   * @param value the short property value to set in the Message.
   * @throws JMSException                 if JMS fails to  set Property due to
   *                                      some internal JMS error.
   * @throws MessageNotWriteableException if properties are read-only
   */
  public void setShortProperty(String name, short value) throws JMSException
  {
    if (readOnly)
    {
      throw new MessageNotWriteableException("Message properties are read only");
    }
    verifyName(name);
    checkProps();
    props.setShort(name, value);
    propBytes = null;
  }

  /**
   * Set an integer property value with the given name, into the Message.
   *
   * @param name  the name of the integer property
   * @param value the integer property value to set in the Message.
   * @throws JMSException                 if JMS fails to  set Property due to
   *                                      some internal JMS error.
   * @throws MessageNotWriteableException if properties are read-only
   */
  public void setIntProperty(String name, int value) throws JMSException
  {
    if (readOnly)
    {
      throw new MessageNotWriteableException("Message properties are read only");
    }
    verifyName(name);
    if (name != null && name.equals(PROP_DELIVERY_COUNT))
      deliveryCount = value;
    else
    {
      checkProps();
      props.setInt(name, value);
      propBytes = null;
    }
  }

  /**
   * Set a long property value with the given name, into the Message.
   *
   * @param name  the name of the long property
   * @param value the long property value to set in the Message.
   * @throws JMSException                 if JMS fails to  set Property due to
   *                                      some internal JMS error.
   * @throws MessageNotWriteableException if properties are read-only
   */
  public void setLongProperty(String name, long value) throws JMSException
  {
    if (readOnly)
    {
      throw new MessageNotWriteableException("Message properties are read only");
    }
    verifyName(name);
    checkProps();
    props.setLong(name, value);
    propBytes = null;
  }

  /**
   * Set a float property value with the given name, into the Message.
   *
   * @param name  the name of the float property
   * @param value the float property value to set in the Message.
   * @throws JMSException                 if JMS fails to  set Property due to
   *                                      some internal JMS error.
   * @throws MessageNotWriteableException if properties are read-only
   */
  public void setFloatProperty(String name, float value) throws JMSException
  {
    if (readOnly)
    {
      throw new MessageNotWriteableException("Message properties are read only");
    }
    verifyName(name);
    checkProps();
    props.setFloat(name, value);
    propBytes = null;
  }

  /**
   * Set a double property value with the given name, into the Message.
   *
   * @param name  the name of the double property
   * @param value the double property value to set in the Message.
   * @throws JMSException                 if JMS fails to  set Property due to
   *                                      some internal JMS error.
   * @throws MessageNotWriteableException if properties are read-only
   */
  public void setDoubleProperty(String name, double value) throws JMSException
  {
    if (readOnly)
    {
      throw new MessageNotWriteableException("Message properties are read only");
    }
    verifyName(name);
    checkProps();
    props.setDouble(name, value);
    propBytes = null;
  }

  /**
   * Set a String property value with the given name, into the Message.
   *
   * @param name  the name of the String property
   * @param value the String property value to set in the Message.
   * @throws JMSException                 if JMS fails to  set Property due to
   *                                      some internal JMS error.
   * @throws MessageNotWriteableException if properties are read-only
   */
  public void setStringProperty(String name, String value) throws JMSException
  {
    if (readOnly)
    {
      throw new MessageNotWriteableException("Message properties are read only");
    }
    verifyName(name);
    if (value != null && value.length() > 65535)
      throw new JMSException("Maximum length of a String property is 65535! This value has a length of " + value.length());
    if (name != null && name.equals(PROP_USER_ID))
      userId = value != null ? new LazyUTF8String(value) : null;
    else if (name != null && name.equals(PROP_CLIENT_ID))
      clientId = value != null ? new LazyUTF8String(value) : null;
    else
    {
      checkProps();
      if (value == null)
        props.remove(name);
      else
        props.setString(name, value);
      propBytes = null;
    }
  }

  /**
   * Set a Java object property value with the given name, into the Message.
   * <p/>
   * <P>Note that this method only works for the objectified primitive
   * object types (Integer, Double, Long ...) and String's.
   *
   * @param name  the name of the Java object property.
   * @param value the Java object property value to set in the Message.
   * @throws JMSException                 if JMS fails to  set Property due to
   *                                      some internal JMS error.
   * @throws MessageFormatException       if object is invalid
   * @throws MessageNotWriteableException if properties are read-only
   */
  public void setObjectProperty(String name, Object value) throws JMSException
  {
    if (readOnly)
    {
      throw new MessageNotWriteableException("Message properties are read only");
    }
    verifyName(name);
    checkProps();
    if (value == null)
      props.remove(name);
    else
    {
      if (value instanceof String && ((String) value).length() > 65535)
        throw new JMSException("Maximum length of a String property is 65535! This value has a length of " + ((String) value).length());
      props.setObject(name, value, false);
    }
    propBytes = null;
  }

  /**
   * Acknowledge this and all previous messages received.
   * <p/>
   * <P>All JMS messages support the acknowledge() method for use when a
   * client has specified that a JMS consumers messages are to be
   * explicitly acknowledged.
   * <p/>
   * <P>JMS defaults to implicit message acknowledgement. In this mode,
   * calls to acknowledge() are ignored.
   * <p/>
   * <P>Acknowledgment of a message automatically acknowledges all
   * messages previously received by the consumer. Clients may
   * individually acknowledge messages or they may choose to acknowledge
   * messages in application defined groups (which is done by acknowledging
   * the last received message in the group).
   * <p/>
   * <P>Messages that have been received but not acknowledged may be
   * redelivered to the consumer.
   *
   * @throws JMSException if JMS fails to acknowledge due to some
   *                      internal JMS error.
   */
  public void acknowledge() throws JMSException
  {
    if (myConsumer != null)
    {
      if (MessageTracker.enabled)
      {
        MessageTracker.getInstance().track(this, new String[]{myConsumer.toString()}, "acknowledge ...");
      }
      cancelled = myConsumer.acknowledgeMessage(this);
      if (MessageTracker.enabled)
      {
        MessageTracker.getInstance().track(this, new String[]{myConsumer.toString()}, "acknowledge done, cancelled=" + cancelled);
      }
    } else if (mySession != null)
    {
      if (MessageTracker.enabled)
      {
        MessageTracker.getInstance().track(this, new String[]{mySession.toString()}, "acknowledge ...");
      }
      cancelled = mySession.acknowledgeMessage(messageIndex);
      if (MessageTracker.enabled)
      {
        MessageTracker.getInstance().track(this, new String[]{mySession.toString()}, "acknowledge done, cancelled=" + cancelled);
      }
    }
  }

  /**
   * Clear out the message body. All other parts of the message are left
   * untouched.
   *
   * @throws JMSException if JMS fails to due to some internal JMS error.
   */
  public void clearBody() throws JMSException
  {
  }

  public void reset() throws JMSException
  {
  }

  public String toString()
  {
    StringBuffer s = new StringBuffer();
    s.append(super.toString());
    s.append("\nmessageIndex = ");
    s.append(messageIndex);
    s.append("\nmessageId = ");
    s.append(messageId);
    s.append("\nuserId = ");
    s.append(userId);
    s.append("\nclientId = ");
    s.append(clientId);
    s.append("\ntimeStamp = ");
    s.append(timeStamp);
    s.append("\ncorrelationId = ");
    s.append(correlationId);
    s.append("\nreplyTo = ");
    s.append(replyTo);
    s.append("\ndestination = ");
    s.append(destination);
    s.append("\ndeliveryMode = ");
    s.append(deliveryMode);
    s.append("\nredelivered = ");
    s.append(redelivered);
    s.append("\ndeliveryCount = ");
    s.append(deliveryCount);
    s.append("\ntype = ");
    s.append(type);
    s.append("\nexpiration = ");
    s.append(expiration);
    s.append("\npriority = ");
    s.append(priority);
    s.append("\nprops = ");
    s.append(props);
    s.append("\nreadOnly = ");
    s.append(readOnly);
    s.append("\nsourceRouter = ");
    s.append(sourceRouter);
    s.append("\ndestRouter = ");
    s.append(destRouter);
    s.append("\ndestQueue = ");
    s.append(destQueue);
    return s.toString();
  }
}



