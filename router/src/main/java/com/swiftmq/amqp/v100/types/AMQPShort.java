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
 * Integer in the range -(2^15) to 2^15-1 inclusive
 *
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPShort extends AMQPType
{
  byte[] bytes = new byte[2];

  /**
   * Constructs an AMQPShort with an undefined value
   *
   */
  public AMQPShort()
  {
    super("short", AMQPTypeDecoder.SHORT);
  }

  /**
   * Constructs an AMQPShort with a value
   *
   * @param value value
   */
  public AMQPShort(int value)
  {
    super("short", AMQPTypeDecoder.SHORT);
    setValue(value);
  }

  /**
   * Sets the value
   *
   * @param value value
   */
  public void setValue(int value)
  {
    Util.writeShort(value, bytes, 0);
  }

  /**
   * Returns the value
   * @return value
   */
  public int getValue()
  {
    return Util.readShort(bytes, 0);
  }

  public int getPredictedSize()
  {
    int n = super.getPredictedSize()+bytes.length;
    return n;
  }

  public void readContent(DataInput in) throws IOException
  {
    in.readFully(bytes);
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    out.write(bytes);
  }

  public String getValueString()
  {
    return Short.toString((short)getValue());
  }

  public String toString()
  {
    return "[AMQPShort, value=" + getValue() + " " + super.toString() + "]";
  }
}