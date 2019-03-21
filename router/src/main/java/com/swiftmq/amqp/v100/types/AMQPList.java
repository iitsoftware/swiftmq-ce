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

package com.swiftmq.amqp.v100.types;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * A sequence of polymorphic values
 *
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPList extends AMQPType
{
  List<AMQPType> list = null;
  byte[] bytes;
  byte[] valueBytes = null;

  /**
   * Constructs an empty AMQPList
   *
   */
  public AMQPList()
  {
    super("list", AMQPTypeDecoder.UNKNOWN);
  }

  /**
   * Constructs an AMQPList with an initial value.
   *
   * @param list initial value
   * @throws IOException on encode error
   */
  public AMQPList(List<AMQPType> list) throws IOException
  {
    super("list", AMQPTypeDecoder.UNKNOWN);
    setValue(list);
  }

  /**
   * Sets the value
   *
   * @param list value
   * @throws IOException on encode error
   */
  public void setValue(List<AMQPType> list) throws IOException
  {
    this.list = list;
    if (list != null)
    {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(bos);
      for (int i = 0; i < list.size(); i++)
        list.get(i).writeContent(dos);
      dos.close();
      valueBytes = bos.toByteArray();
      bytes = null;
      if (list.size() == 0)
        code = AMQPTypeDecoder.LIST0;
      else
      {
        if (valueBytes.length > 254 || list.size() > 255)
          code = AMQPTypeDecoder.LIST32;
        else
          code = AMQPTypeDecoder.LIST8;
      }
    }
  }

  /**
   * Returns the value.
   *
   * @return value
   * @throws IOException on decode error
   */
  public List<AMQPType> getValue() throws IOException
  {
    if (list == null)
    {
      list = new ArrayList<AMQPType>();
      if (code != AMQPTypeDecoder.LIST0)
      {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
        int n = 0;
        if (code == AMQPTypeDecoder.LIST8)
          n = dis.readByte() & 0xff;
        else if (code == AMQPTypeDecoder.LIST32)
          n = dis.readInt();
        else
          throw new IOException("Invalid code: " + code);
        if (n < 0)
          throw new IOException("Invalid list element count: " + n);
        for (int i = 0; i < n; i++)
          list.add(AMQPTypeDecoder.decode(dis));
      }
    }
    return list;
  }

  public int getPredictedSize()
  {
    int n = super.getPredictedSize();
    if (code == AMQPTypeDecoder.LIST0)
      return n;
    if (bytes != null)
      n += bytes.length;
    else
    {
      if (code == AMQPTypeDecoder.LIST8)
        n += 1;
      else
        n += 4;
      if (valueBytes != null)
        n += valueBytes.length;
    }
    return n;
  }

  public void readContent(DataInput in) throws IOException
  {
    if (code == AMQPTypeDecoder.LIST0)
      return;
    int len = 0;
    if (code == AMQPTypeDecoder.LIST8)
      len = in.readByte() & 0xff;
    else if (code == AMQPTypeDecoder.LIST32)
      len = in.readInt();
    else
      throw new IOException("Invalid code: " + code);
    if (len < 0)
      throw new IOException("byte[] array length invalid: " + len);
    bytes = new byte[len];
    in.readFully(bytes);
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    if (code == AMQPTypeDecoder.LIST0)
      return;
    if (bytes != null)
    {
      if (code == AMQPTypeDecoder.LIST8)
        out.writeByte(bytes.length);
      else if (code == AMQPTypeDecoder.LIST32)
        out.writeInt(bytes.length);
      out.write(bytes);
    } else
    {
      if (code == AMQPTypeDecoder.LIST8)
      {
        out.writeByte(valueBytes.length + 1);
        out.writeByte(list.size());
      } else if (code == AMQPTypeDecoder.LIST32)
      {
        out.writeInt(valueBytes.length + 4);
        out.writeInt(list.size());
      }
      out.write(valueBytes);
    }
  }

  private String printList()
  {
    if (list == null)
      return null;
    StringBuffer b = new StringBuffer("\n");
    for (int i = 0; i < list.size(); i++)
    {
      b.append(i);
      b.append(": ");
      b.append(list.get(i));
      b.append("\n");
    }
    return b.toString();
  }

  public String getValueString()
  {
    try
    {
      getValue();
    } catch (IOException e)
    {
      e.printStackTrace();
    }
    StringBuffer b = new StringBuffer("[");
    for (int i = 0; i < list.size(); i++)
    {
      if (i > 0)
        b.append(", ");
      b.append(((AMQPType) list.get(i)).getValueString());
    }
    b.append("]");
    return b.toString();
  }

  public String toString()
  {
    return "[AMQPList, list=" + printList() + ", bytes.length=" + (bytes != null ? bytes.length : "null") + ", valueBytes.length=" + (valueBytes != null ? valueBytes.length : "null") + super.toString() + "]";
  }
}