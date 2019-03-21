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

package com.swiftmq.amqp.v100.generated.messaging.addressing;

import com.swiftmq.amqp.v100.types.*;
import com.swiftmq.amqp.v100.transport.*;
import com.swiftmq.amqp.v100.generated.*;
import com.swiftmq.amqp.v100.generated.transport.definitions.Error;
import com.swiftmq.amqp.v100.generated.transport.performatives.*;
import com.swiftmq.amqp.v100.generated.transport.definitions.*;
import com.swiftmq.amqp.v100.generated.messaging.message_format.*;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.*;
import com.swiftmq.amqp.v100.generated.security.sasl.*;
import com.swiftmq.amqp.v100.generated.transactions.coordination.*;
import com.swiftmq.amqp.v100.generated.provides.global_tx_id_types.*;
import com.swiftmq.amqp.v100.generated.filter.filter_types.*;
import java.io.*;
import java.util.*;

/**
 * <p>
 * </p><p>
 * A node dynamically created with this lifetime policy will be deleted at the point that
 * there remain no links for which the node is either the source or target.
 * </p><p>
 * </p><p>
 * </p>
 *
 *  @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 **/

public class DeleteOnNoLinks extends AMQPList
       implements LifetimePolicyIF
{
  public static String DESCRIPTOR_NAME = "amqp:delete-on-no-links:list";
  public static long DESCRIPTOR_CODE = 0x00000000L<<32 | 0x0000002cL;

  public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
  public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);

  boolean dirty = false;


  /**
   * Constructs a DeleteOnNoLinks.
   *
   * @param initValue initial value
   * @exception error during initialization
   */
  public DeleteOnNoLinks(List initValue) throws Exception
  {
    super(initValue);
    if (initValue != null)
      decode();
  }

  /**
   * Constructs a DeleteOnNoLinks.
   *
   */
  public DeleteOnNoLinks()
  {
    dirty = true;
  }

  /**
   * Return whether this DeleteOnNoLinks has a descriptor
   *
   * @return true/false
   */
  public boolean hasDescriptor()
  {
    return true;
  }

  /**
   * Accept method for a LifetimePolicy visitor.
   *
   * @param visitor LifetimePolicy visitor
   */
  public void accept(LifetimePolicyVisitor visitor)
  {
    visitor.visit(this);
  }


  /**
   * Returns the predicted size of this DeleteOnNoLinks. The predicted size may be greater than the actual size
   * but it can never be less.
   *
   * @return predicted size
   */
  public int getPredictedSize()
  {
    int n;
    if (dirty)
    {
      AMQPDescribedConstructor _c = getConstructor();
      setConstructor(null);
      n = super.getPredictedSize();
      n += codeConstructor.getPredictedSize();
    setConstructor(_c);
    } else
      n = super.getPredictedSize();
    return n;
  }

  private AMQPArray singleToArray(AMQPType t) throws IOException
  {
    return new AMQPArray(t.getCode(), new AMQPType[]{t});
  }

  private void decode() throws Exception
  {
    List l = getValue();

    AMQPType t = null;
    int idx = 0;
  }

  private void addToList(List list, Object value)
  {
    if (value != null)
      list.add(value);
    else
      list.add(AMQPNull.NULL);
  }

  private void encode() throws IOException
  {
    List l = new ArrayList();
    for (ListIterator iter=l.listIterator(l.size());iter.hasPrevious();)
    {
        AMQPType t = (AMQPType)iter.previous();
        if (t.getCode() == AMQPTypeDecoder.NULL)
          iter.remove();
        else
          break;
    }
    setValue(l);
    dirty = false;
  }

  /**
   * Returns an array constructor (internal use)
   *
   * @return array constructor
   */
  public AMQPDescribedConstructor getArrayConstructor() throws IOException
  {
    if (dirty)
      encode();
    codeConstructor.setFormatCode(getCode());
    return codeConstructor;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    if (dirty)
      encode();
    if (getConstructor() != codeConstructor)
    {
      codeConstructor.setFormatCode(getCode());
      setConstructor(codeConstructor);
    }
    super.writeContent(out);
  }

  public String getValueString()
  {
    try
    {
      if (dirty)
        encode();
    } catch (IOException e)
    {
      e.printStackTrace();
    }
    StringBuffer b = new StringBuffer("[DeleteOnNoLinks ");
    b.append(getDisplayString());
    b.append("]");
    return b.toString();
  }

  private String getDisplayString()
  {
    boolean _first = true;
    StringBuffer b = new StringBuffer();
    return b.toString();
  }

  public String toString()
  {
    return "[DeleteOnNoLinks " + getDisplayString() + "]";
  }
}
