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

import com.swiftmq.jndi.SwiftMQObjectFactory;
import com.swiftmq.tools.util.LazyUTF8String;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Implementation of a Queue.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class QueueImpl implements Queue, Referenceable, Serializable, DestinationImpl
{
  LazyUTF8String queueName = null;

  /**
   * Creates a new QueueImpl.
   *
   * @param queueName queue name.
   */
  public QueueImpl(String queueName)
  {
    setQueueName(queueName);
  }

  /**
   * Creates a new QueueImpl.
   */
  public QueueImpl()
  {
  }

  public Reference getReference() throws NamingException
  {
    return new Reference(QueueImpl.class.getName(),
        new StringRefAddr("queueName", queueName != null ? queueName.getString() : null),
        SwiftMQObjectFactory.class.getName(),
        null);
  }

  public void unfoldBuffers()
  {
    if (queueName != null)
      queueName.getString(true);
  }

  public int getType()
  {
    return DestinationFactory.TYPE_QUEUE;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    queueName.writeContent(out);
  }

  public void readContent(DataInput in) throws IOException
  {
    queueName = new LazyUTF8String(in);
  }

  /**
   * Set the queue name.
   *
   * @param queueName queue name.
   */
  public void setQueueName(String queueName)
  {
    this.queueName = queueName != null ? new LazyUTF8String(queueName) : null;
  }

  /**
   * Get the name of this queue.
   * <p/>
   * <P>Clients that depend upon the name, are not portable.
   *
   * @return the queue name
   * @throws JMSException if JMS implementation for Queue fails to
   *                      to return queue name due to some internal
   *                      error.
   */
  public String getQueueName() throws JMSException
  {
    return queueName != null ? queueName.getString() : null;
  }

  /**
   * Return a pretty printed version of the queue name
   *
   * @return the provider specific identity values for this queue.
   */
  public String toString()
  {
    return queueName != null ? queueName.getString() : null;
  }
}



