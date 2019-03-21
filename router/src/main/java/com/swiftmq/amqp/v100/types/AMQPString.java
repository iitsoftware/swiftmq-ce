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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * A sequence of unicode characters
 *
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPString extends AMQPType implements Comparable
{
  String value = null;
  byte[] bytes;

  /**
   * Constructs an AMQPString with an undefined value
   *
   */
  public AMQPString()
  {
    super("string", AMQPTypeDecoder.UNKNOWN);
  }

  /**
   * Constructs an AMQPString with a value
   *
   * @param value value
   */
  public AMQPString(String value)
  {
    this();
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
      Charset charset = Charset.forName("UTF-8");
      ByteBuffer buffer = charset.encode(value);
      bytes = new byte[buffer.limit()];
      buffer.get(bytes);
      if (bytes.length > 255)
        code = AMQPTypeDecoder.STR32UTF8;
      else
        code = AMQPTypeDecoder.STR8UTF8;
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
      Charset charset = Charset.forName("UTF-8");
      value = charset.decode(ByteBuffer.wrap(bytes)).toString();
    }
    return value;
  }

  public int getPredictedSize()
  {
    int n = super.getPredictedSize();
    if (code == AMQPTypeDecoder.STR8UTF8)
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
    if (code == AMQPTypeDecoder.STR8UTF8)
      len = in.readUnsignedByte();
    else if (code == AMQPTypeDecoder.STR32UTF8)
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
    if (code == AMQPTypeDecoder.STR8UTF8)
      out.writeByte(bytes.length);
    else if (code == AMQPTypeDecoder.STR32UTF8)
      out.writeInt(bytes.length);
    out.write(bytes);
  }

  public boolean equals(Object o)
  {
    return getValue().equals(((AMQPString) o).getValue());
  }

  public int compareTo(Object o)
  {
    return getValue().compareTo(((AMQPString) o).getValue());
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
    return "[AMQPString, value=" + getValue() + ", bytes.length=" + (bytes != null ? bytes.length : "null") + " " + super.toString() + "]";
  }
}