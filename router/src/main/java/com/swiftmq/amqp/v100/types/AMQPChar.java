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
 * A single unicode character
 *
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPChar extends AMQPType
{
  int value;

  /**
   * Constructs an AMQPChar with an undefined value
   *
   */
  public AMQPChar()
  {
    super("char", AMQPTypeDecoder.CHAR);
  }

  /**
   * Constructs an AMQPChar with a value
   *
   * @param value value
   */
  public AMQPChar(int value)
  {
    super("char", AMQPTypeDecoder.CHAR);
    setValue(value);
  }

  /**
   * Sets the value
   *
   * @param value value
   */
  public void setValue(int value)
  {
    this.value = value;
  }

  /**
   * Returns the value
   * @return  value
   */
  public int getValue()
  {
    return value;
  }

  public int getPredictedSize()
  {
    int n = super.getPredictedSize()+4;
    return n;
  }

  public void readContent(DataInput in) throws IOException
  {
    value = in.readInt();
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    out.writeInt(value);
  }

  public String getValueString()
  {
    return String.valueOf(value);
  }

  public String toString()
  {
    return "[AMQPChar, value=" + getValue() + " " + super.toString() + "]";
  }
}