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

package com.swiftmq.amqp.v100.generated.messaging.message_format;

import com.swiftmq.amqp.v100.types.*;
import com.swiftmq.amqp.v100.transport.*;
import com.swiftmq.amqp.v100.generated.*;
import com.swiftmq.amqp.v100.generated.transport.definitions.Error;
import com.swiftmq.amqp.v100.generated.transport.performatives.*;
import com.swiftmq.amqp.v100.generated.transport.definitions.*;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.*;
import com.swiftmq.amqp.v100.generated.messaging.addressing.*;
import com.swiftmq.amqp.v100.generated.security.sasl.*;
import com.swiftmq.amqp.v100.generated.transactions.coordination.*;
import com.swiftmq.amqp.v100.generated.provides.global_tx_id_types.*;
import com.swiftmq.amqp.v100.generated.filter.filter_types.*;
import java.io.*;
import java.util.*;

/**
 * <p>
 * </p><p>
 * A data section contains opaque binary data.
 * </p><p>
 * </p><p>
 * </p>
 *
 *  @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 **/

public class Data extends AMQPBinary
       implements SectionIF
{
  public static String DESCRIPTOR_NAME = "amqp:data:binary";
  public static long DESCRIPTOR_CODE = 0x00000000L<<32 | 0x00000075L;

  public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
  public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);


  /**
   * Constructs a Data.
   *
   * @param initValue initial value
   */
  public Data(byte[] initValue) 
  {
    super(initValue);
  }

  /**
   * Accept method for a Section visitor.
   *
   * @param visitor Section visitor
   */
   public void accept(SectionVisitor visitor)
   {
     visitor.visit(this);
   }

  /**
   * Return whether this Data has a descriptor
   *
   * @return true/false
   */
  public boolean hasDescriptor()
  {
    return true;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    if (getConstructor() != codeConstructor)
    {
      codeConstructor.setFormatCode(getCode());
      setConstructor(codeConstructor);
    }
    super.writeContent(out);
  }

  public String toString()
  {
    return "[Data " + super.toString() + "]";
  }
}
