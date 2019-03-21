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
import java.math.BigDecimal;
import java.math.MathContext;

/**
 * 32-bit decimal number (IEEE 754-2008 decimal32)
 *
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPDecimal32 extends AMQPType
{
  byte[] bytes = new byte[4];

  /**
   * Constructs an AMQPDecimal32 with an undefined value
   *
   */
  public AMQPDecimal32()
  {
    super("decimal32", AMQPTypeDecoder.DECIMAL32);
  }

  /**
   * Constructs an AMQPDecimal32 with a value
   *
   * @param value value
   */
  public AMQPDecimal32(BigDecimal value)
  {
    super("decimal32", AMQPTypeDecoder.DECIMAL32);
    setValue(value);
  }

  /**
   * Sets the value
   *
   * @param value value
   */
  public void setValue(BigDecimal value)
  {
    Util.writeInt(value.intValue(), bytes, 0);
  }

  /**
   * Returns the value
   * @return  value
   */
  public BigDecimal getValue()
  {
    return new BigDecimal(Util.readInt(bytes, 0), MathContext.DECIMAL32);
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
    return getValue().toString();
  }

  public String toString()
  {
    return "[AMQPDecimal32, value=" + getValue() + " " + super.toString() + "]";
  }
}
