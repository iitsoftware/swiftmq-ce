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

package com.swiftmq.swiftlet.queue;

import com.swiftmq.jms.BytesMessageImpl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A QueueInputStream is an input stream that is mapped to a queue. Together with
 * a <code>QueueOutputStream</code>, it enables queue based streaming.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 * @see QueueOutputStream
 */
public class QueueInputStream extends InputStream
{
  QueueReceiver queueReceiver = null;
  QueuePullTransaction transaction = null;
  BytesMessageImpl currentMsg = null;
  int actSize = -1;
  int count = 0;
  int actSeq = -1;
  boolean eof = false;
  long receiveTimeout = -1;
  int windowSize = 10;
  SortedSet messageCache = null;

  /**
   * Constructs a new QueueInputStream.
   * The queue receiver works as factory for transactions (every message read from
   * the queue is wrapped in a single transaction). The receive timeout ensures that
   * a receive doesn't block forever. If a timeout occurs, it will be turned into an
   * IOException of the <code>read()</code> method. The window size specifies a backlog
   * of messages to receive, if messages are out of sequence. If the size is reached and
   * the stream is still out of sequence, an IOException will be thrown.
   * @param queueReceiver receiver.
   * @param receiveTimeout timeout.
   * @param windowSize window size.
   * @return description.
   */
  public QueueInputStream(QueueReceiver queueReceiver, long receiveTimeout, int windowSize)
  {
    // SBgen: Assign variables
    this.queueReceiver = queueReceiver;
    this.receiveTimeout = receiveTimeout;
    this.windowSize = windowSize;
    // SBgen: End assign
    messageCache = new TreeSet(new Comparator()
    {
      public int compare(Object o1, Object o2)
      {
        int i1, i2;
        try
        {
          i1 = ((BytesMessageImpl) o1).getIntProperty(QueueOutputStream.SEQNO);
          i2 = ((BytesMessageImpl) o2).getIntProperty(QueueOutputStream.SEQNO);
          return i1 == i2?0:i1 < i2?-1:1;
        } catch (Exception e)
        {
          e.printStackTrace();
        }
        return 0;
      }

      public boolean equals(Object that)
      {
        return false;
      }
    });
  }


  /**
   * Set recive timeout.
   * @param receiveTimeout receive timeout.
   */
  public void setReceiveTimeout(long receiveTimeout)
  {
    // SBgen: Assign variable
    this.receiveTimeout = receiveTimeout;
  }


  /**
   * Returns the receive timeout.
   * @return receive timeout.
   */
  public long getReceiveTimeout()
  {
    // SBgen: Get variable
    return (receiveTimeout);
  }


  /**
   * Sets the window size.
   * @param windowSize window size.
   */
  public void setWindowSize(int windowSize)
  {
    // SBgen: Assign variable
    this.windowSize = windowSize;
  }


  /**
   * Returns the window size.
   * @return window size.
   */
  public int getWindowSize()
  {
    // SBgen: Get variable
    return (windowSize);
  }


  /**
   * Returns the names of the message properties.
   * @return names.
   * @exception Exception on error.
   */
  public Enumeration getMsgPropNames() throws Exception
  {
    if (currentMsg == null)
      return null;
    return currentMsg.getPropertyNames();
  }


  /**
   * Returns a message property.
   * @param key key.
   * @return value.
   * @exception Exception on error.
   */
  public Object getMsgProp(String key) throws Exception
  {
    if (currentMsg == null)
      return null;
    return currentMsg.getObjectProperty(key);
  }

  private boolean isEOF(BytesMessageImpl msg)
  {
    try
    {
      boolean b = msg.getBooleanProperty(QueueOutputStream.EOF);
      return b;
    } catch (Exception e)
    {
      return false;
    }
  }

  private void ensureNextMsgInSequence() throws IOException
  {
    if (currentMsg != null && (count < actSize || eof))
      return;

    currentMsg = null;
    if (messageCache.size() > 0)
    {
      try
      {
        MessageEntry me = (MessageEntry) messageCache.first();
        BytesMessageImpl msg = (BytesMessageImpl) me.message;
        int seqNo = msg.getIntProperty(QueueOutputStream.SEQNO);
        if (seqNo == actSeq + 1)
        {
          currentMsg = msg;
          actSeq = seqNo;
          actSize = msg.getIntProperty(QueueOutputStream.SIZE);
          eof = isEOF(msg);
          count = 0;
          messageCache.remove(msg);
          return;
        }
      } catch (Exception e)
      {
        throw new IOException(e.toString());
      }
    }
    while (currentMsg == null && messageCache.size() < windowSize)
    {
      try
      {
        transaction = queueReceiver.createTransaction(false);
        BytesMessageImpl msg = null;
        MessageEntry me = null;
        if (receiveTimeout > 0)
          me = transaction.getMessage(receiveTimeout);
        else
          me = transaction.getMessage();
        msg = (BytesMessageImpl) me.message;
        msg.reset();
        int seqNo = msg.getIntProperty(QueueOutputStream.SEQNO);
        if (seqNo == actSeq + 1)
        {
          currentMsg = msg;
          actSeq = seqNo;
          actSize = msg.getIntProperty(QueueOutputStream.SIZE);
          eof = isEOF(msg);
          count = 0;
        } else
          messageCache.add(msg);
        transaction.commit();
      } catch (Exception e)
      {
        throw new IOException(e.toString());
      }
    }
  }

  /**
   * Reads the next byte of data from the input stream. The value byte is
   * returned as an <code>int</code> in the range <code>0</code> to
   * <code>255</code>. If no byte is available because the end of the stream
   * has been reached, the value <code>-1</code> is returned. This method
   * blocks until input data is available, the end of the stream is detected,
   * or an exception is thrown.
   *
   * @return the next byte of data, or <code>-1</code> if the end of the
   *             stream is reached.
   * @exception IOException if an I/O error occurs.
   */
  public int read()
      throws IOException
  {
    ensureNextMsgInSequence();
    if (eof && count >= actSize)
      return -1;
    int b;
    try
    {
      b = currentMsg.readByte() & 0xff;
      count++;
    } catch (Exception e)
    {
      throw new IOException(e.toString());
    }
    return b;
  }

  /**
   * Returns the number of bytes that can be read (or skipped over) from
   * this input stream without blocking by the next caller of a method for
   * this input stream.  The next caller might be the same thread or or
   * another thread.
   *
   * @return     the number of bytes that can be read from this input stream
   *             without blocking.
   * @exception  IOException  if an I/O error occurs.
   */
  public int available() throws IOException
  {
    if (currentMsg == null)
      return 0;
    return actSize - count;
  }

  /**
   * Closes this input stream and releases any system resources associated
   * with the stream.
   *
   * @exception  IOException  if an I/O error occurs.
   */
  public void close() throws IOException
  {
    messageCache.clear();
    if (transaction != null)
    {
      try
      {
        transaction.rollback();
      } catch (Exception ignored)
      {
      }
    }
  }
}

