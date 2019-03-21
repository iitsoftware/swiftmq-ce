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

/**
 * Integer in the range -(2^31) to 2^31-1 inclusive
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPInt extends AMQPType
{
  byte[] bytes = new byte[4];

  /**
   * Constructs an AMQPInt with an undefined value
   */
  public AMQPInt()
  {
    super("int", AMQPTypeDecoder.INT);
  }

  /**
   * Constructs an AMQPInt with a value
   *
   * @param value value
   */
  public AMQPInt(int value)
  {
    super("int", AMQPTypeDecoder.INT);
    setValue(value);
  }

  /**
   * Sets the value
   *
   * @param value value
   */
  public void setValue(int value)
  {
    Util.writeInt(value, bytes, 0);
    code = AMQPTypeDecoder.INT;
  }

  /**
   * Returns the value
   *
   * @return value
   */
  public int getValue()
  {
    if (code == AMQPTypeDecoder.INT)
      return Util.readInt(bytes, 0);
    return (int) bytes[0];
  }

  public int getPredictedSize()
  {
    int n = super.getPredictedSize();
    if (code == AMQPTypeDecoder.INT)
      n += 4;
    else if (code == AMQPTypeDecoder.SINT)
      n += 1;
    return n;
  }

  public void readContent(DataInput in) throws IOException
  {
    if (code == AMQPTypeDecoder.INT)
      in.readFully(bytes);
    else if (code == AMQPTypeDecoder.SINT)
      bytes[0] = in.readByte();
    else
      throw new IOException("Invalid code: " + code);
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    if (code == AMQPTypeDecoder.INT)
      out.write(bytes);
    else
      out.writeByte(bytes[0]);
  }

  public String getValueString()
  {
    return Integer.toString(getValue());
  }

  public String toString()
  {
    return "[AMQPInt, value=" + getValue() + " " + super.toString() + "]";
  }
}
