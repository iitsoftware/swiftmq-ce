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
 * An absolute point in time
 *
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPTimestamp extends AMQPType
{
  byte[] bytes = new byte[8];

  /**
   * Constructs an AMQPTimestamp with an undefined value
   *
   */
  public AMQPTimestamp()
  {
    super("timestamp", AMQPTypeDecoder.TIMESTAMP);
  }

  /**
   * Constructs an AMQPTimestamp with a value
   *
   * @param value value
   */
  public AMQPTimestamp(long value)
  {
    super("timestamp", AMQPTypeDecoder.TIMESTAMP);
    setValue(value);
  }

  /**
   * Sets the value
   *
   * @param value value
   */
  public void setValue(long value)
  {
    Util.writeLong(value, bytes, 0);
  }

  /**
   * Returns the value
   * @return value
   */
  public long getValue()
  {
    return Util.readLong(bytes, 0);
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
    return String.valueOf(getValue());
  }

  public String toString()
  {
    return "[AMQPTimestamp, value=" + getValue() + " " + super.toString() + "]";
  }
}