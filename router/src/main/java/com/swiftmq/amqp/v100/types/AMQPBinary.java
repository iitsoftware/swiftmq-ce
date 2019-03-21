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
import java.util.Arrays;

/**
 * A sequence of octets
 *
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPBinary extends AMQPType
{
  byte[] value;
  String valueString = null;
  int hashCode = -1;

  /**
   * Constructs an empty AMQPBinary
   *
   */
  public AMQPBinary()
  {
    super("binary", AMQPTypeDecoder.UNKNOWN);
  }

  /**
   * Constructs an AMQPBinary with a value
   *
   * @param value value
   */
  public AMQPBinary(byte[] value)
  {
    super("binary", AMQPTypeDecoder.UNKNOWN);
    setValue(value);
  }

  /**
   * Sets the value.
   *
   * @param value value
   */
  public void setValue(byte[] value)
  {
    this.value = value;
    if (value.length > 255)
      code = AMQPTypeDecoder.BIN32;
    else
      code = AMQPTypeDecoder.BIN8;
    hashCode = Arrays.hashCode(value);
  }

  /**
   * Returns the value
   *
   * @return value
   */
  public byte[] getValue()
  {
    return value;
  }

  public int getPredictedSize()
  {
    int n = super.getPredictedSize();
    if (code == AMQPTypeDecoder.BIN8)
      n += 1;
    else
      n += 4;
    if (value != null)
      n += value.length;
    return n;
  }

  public void readContent(DataInput in) throws IOException
  {
    int len = 0;
    if (code == AMQPTypeDecoder.BIN8)
      len = in.readUnsignedByte();
    else if (code == AMQPTypeDecoder.BIN32)
      len = in.readInt();
    else
      throw new IOException("Invalid code: " + code);
    if (len < 0)
      throw new IOException("byte[] array length invalid: " + len);
    value = new byte[len];
    in.readFully(value);
    hashCode = Arrays.hashCode(value);
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    if (code == AMQPTypeDecoder.BIN8)
      out.writeByte(value.length);
    else
      out.writeInt(value.length);
    out.write(value);
  }

  public String getValueString()
  {
    if (valueString == null)
    {
      if (value == null)
        valueString = "null";
      else
      {
        StringBuffer b = new StringBuffer();
        int len = Math.min(255, value.length);
        for (int i=0;i<len;i++)
          b.append(String.format("%02X", value[i]));
        if (value.length > 255)
          b.append("...");
        valueString = b.toString();
      }
    }
    return valueString;
  }

  public int hashCode()
  {
    return hashCode;
  }

  public boolean equals(Object o)
  {
    if (!(o instanceof AMQPBinary))
      return false;
    return Arrays.equals(((AMQPBinary)o).getValue(), value);
  }

  public String toString()
  {
    return "[AMQPBinary, value.length=" + (value == null ? "null" : value.length) + " " + super.toString() + "]";
  }
}