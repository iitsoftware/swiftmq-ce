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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Symbolic values from a constrained domain
 *
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPSymbol extends AMQPType implements Comparable
{
  String value = null;
  byte[] bytes;

  /**
   * Constructs an AMQPSymbol with an undefined value
   *
   */
  public AMQPSymbol()
  {
    super("symbol", AMQPTypeDecoder.SYM32);
  }

  /**
   * Constructs an AMQPSymbol with a value
   *
   * @param value value
   */
  public AMQPSymbol(String value)
  {
    super("symbol", AMQPTypeDecoder.SYM32);
    setValue(value);
  }

  /**
   * Sets the value
   *
   * @param value value
   */
  public void setValue(String value)
  {
    this.value = value;
    if (value != null)
    {
      Charset charset = Charset.forName("US-ASCII");
      ByteBuffer buffer = charset.encode(value);
      bytes = new byte[buffer.limit()];
      buffer.get(bytes);
      code = bytes.length > 255 ? AMQPTypeDecoder.SYM32 : AMQPTypeDecoder.SYM8;
    }
  }

  /**
   * Returns the value
   * @return value
   */
  public String getValue()
  {
    if (value == null)
    {
      Charset charset = Charset.forName("US-ASCII");
      value = charset.decode(ByteBuffer.wrap(bytes)).toString();
    }
    return value;
  }

  public int getPredictedSize()
  {
    int n = super.getPredictedSize();
    if (code == AMQPTypeDecoder.SYM8)
      n += 1;
    else
      n += 4;
    if (bytes != null)
      n += bytes.length;
    return n;
  }

  public void readContent(DataInput in) throws IOException
  {
    int len = 0;
    if (code == AMQPTypeDecoder.SYM8)
      len = in.readUnsignedByte();
    else if (code == AMQPTypeDecoder.SYM32)
      len = in.readInt();
    else
      throw new IOException("Invalid code: " + code);
    if (len < 0)
      throw new IOException("byte[] array length invalid: " + len);
    bytes = new byte[len];
    try
    {
      in.readFully(bytes);
    } catch (Exception e)
    {
      System.out.println("code="+code+", len="+len);
      e.printStackTrace();
    }
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    if (code == AMQPTypeDecoder.SYM8)
      out.writeByte(bytes.length);
    else if (code == AMQPTypeDecoder.SYM32)
      out.writeInt(bytes.length);
    out.write(bytes);
  }

  public boolean equals(Object o)
  {
    return getValue().equals(((AMQPSymbol) o).getValue());
  }

  public int compareTo(Object o)
  {
    return getValue().compareTo(((AMQPSymbol) o).getValue());
  }

  public int hashCode()
  {
    return getValue().hashCode();
  }

  public String getValueString()
  {
    return getValue();
  }

  public String toString()
  {
    return "[AMQPSymbol, value=" + getValue() + ", bytes.length=" + (bytes != null ? bytes.length : "null") + " " + super.toString() + "]";
  }
}