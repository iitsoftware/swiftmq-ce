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

import com.swiftmq.tools.util.LazyUTF8String;

import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;
import javax.jms.TextMessage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implementation of a TextMessage.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class TextMessageImpl extends MessageImpl implements TextMessage
{
  private static int CHUNK_SIZE = 20 * 1024; // 50 KB
  boolean bodyReadOnly = false;
  LazyUTF8String[] lazy = null;

  protected int getType()
  {
    return TYPE_TEXTMESSAGE;
  }

  protected void unfoldBody()
  {
    if (lazy != null)
    {
      for (int i = 0; i < lazy.length; i++)
      {
        lazy[i].getString(true);
      }
    }
  }

  protected void writeBody(DataOutput out) throws IOException
  {
    if (lazy == null)
      out.writeByte(0);
    else
    {
      out.writeByte(1);

      out.writeInt(lazy.length);
      for (int i = 0; i < lazy.length; i++)
      {
        lazy[i].writeContent(out);
      }
    }
  }

  protected void readBody(DataInput in) throws IOException
  {
    byte set = in.readByte();
    if (set == 0)
      lazy = null;
    else
    {
      int nChunks = in.readInt();
      lazy = new LazyUTF8String[nChunks];
      for (int i = 0; i < nChunks; i++)
        lazy[i] = new LazyUTF8String(in);
    }
  }

  /**
   * Set the string containing this message's data.
   *
   * @param s the String containing the message's data
   * @throws JMSException                 if JMS fails to set text due to
   *                                      some internal JMS error.
   * @throws MessageNotWriteableException if message in read-only mode.
   */
  public void setText(String s) throws JMSException
  {
    if (bodyReadOnly)
    {
      throw new MessageNotWriteableException("Message is read only");
    }
    if (s != null)
    {
      int nChunks = (s.length() / CHUNK_SIZE) + 1;
      lazy = new LazyUTF8String[nChunks];
      int pos = 0;
      for (int i = 0; i < nChunks; i++)
      {
        String part = s.substring(pos, Math.min(pos + CHUNK_SIZE, s.length()));
        lazy[i] = new LazyUTF8String(part);
        pos += CHUNK_SIZE;
      }
    } else
      lazy = null;
  }

  /**
   * Get the string containing this message's data.  The default
   * value is null.
   *
   * @return the String containing the message's data
   * @throws JMSException if JMS fails to get text due to
   *                      some internal JMS error.
   */
  public String getText() throws JMSException
  {
    if (lazy == null)
      return null;
    if (lazy.length == 1)
      return lazy[0].getString();
    StringBuffer b = new StringBuffer();
    for (int i = 0; i < lazy.length; i++)
      b.append(lazy[i].getString());
    return b.toString();
  }

  public void setReadOnly(boolean b)
  {
    super.setReadOnly(b);
    bodyReadOnly = b;
  }

  /**
   * Clear out the message body. All other parts of the message are left
   * untouched.
   *
   * @throws JMSException if JMS fails to due to some internal JMS error.
   */
  public void clearBody() throws JMSException
  {
    super.clearBody();
    lazy = null;
    bodyReadOnly = false;
  }

  public String toString()
  {
    StringBuffer b = new StringBuffer("[TextMessageImpl ");
    b.append(super.toString());
    b.append("\n");
    b.append("lazy=");
    b.append(lazy);
    b.append("\n");
    if (lazy != null)
    {
      b.append("lazy.length=");
      b.append(lazy.length);
      b.append("\n");
      for (int i = 0; i < lazy.length; i++)
      {
        b.append("lazy[" + i + "]=");
        b.append(lazy[i]);
        b.append("\n");
      }
    }
    b.append("]");
    return b.toString();
  }

}



