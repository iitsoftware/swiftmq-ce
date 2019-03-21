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

package com.swiftmq.amqp.v100.generated.transport.performatives;

import com.swiftmq.amqp.v100.types.*;
import com.swiftmq.amqp.v100.transport.*;
import com.swiftmq.amqp.v100.generated.*;
import com.swiftmq.amqp.v100.generated.transport.definitions.Error;
import com.swiftmq.amqp.v100.generated.transport.definitions.*;
import com.swiftmq.amqp.v100.generated.messaging.message_format.*;
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
 * Detach the link endpoint from the session. This unmaps the handle and makes it available
 * for use by other links.
 * </p><p>
 * </p><p>
 * </p>
 *
 *  @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 **/

public class DetachFrame extends AMQPFrame
       implements FrameIF
{
  public static String DESCRIPTOR_NAME = "amqp:detach:list";
  public static long DESCRIPTOR_CODE = 0x00000000L<<32 | 0x00000016L;

  public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
  public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);
  
  AMQPList body = null;
  boolean dirty = false;

  Handle handle =  null;
  AMQPBoolean closed =  AMQPBoolean.FALSE;
  Error error =  null;

  /**
   * Constructs a DetachFrame.
   *
   * @param channel the channel id
   * @param body the frame body
   */
  public DetachFrame(int channel, AMQPList body) throws Exception
  {
    super(channel);
    this.body = body;
    if (body != null)
      decode();
  }

  /**
   * Constructs a DetachFrame.
   *
   * @param channel the channel id
   */
  public DetachFrame(int channel)
  {
    super(channel);
  }

  /**
   * Accept method for a Frame visitor.
   *
   * @param visitor Frame visitor
   */
  public void accept(FrameVisitor visitor)
  {
    visitor.visit(this);
  }


  /**
   * Sets the mandatory Handle field.
   *
   * <p>
   * </p>
   * @param handle Handle
   */
  public void setHandle(Handle handle)
  {
    dirty = true;
    this.handle = handle;
  }

  /**
   * Returns the mandatory Handle field.
   *
   * @return Handle
   */
  public Handle getHandle()
  {
    return handle;
  }

  /**
   * Sets the optional Closed field.
   *
   * <p>
   * </p><p>
   * See  .
   * </p><p>
   * </p><p>
   * </p>
   * @param closed Closed
   */
  public void setClosed(AMQPBoolean closed)
  {
    dirty = true;
    this.closed = closed;
  }

  /**
   * Returns the optional Closed field.
   *
   * @return Closed
   */
  public AMQPBoolean getClosed()
  {
    return closed;
  }

  /**
   * Sets the optional Error field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * If set, this field indicates that the link is being detached due to an error condition.
   * The value of the field should contain details on the cause of the error.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param error Error
   */
  public void setError(Error error)
  {
    dirty = true;
    this.error = error;
  }

  /**
   * Returns the optional Error field.
   *
   * @return Error
   */
  public Error getError()
  {
    return error;
  }


  /**
   * Returns the predicted size of this DetachFrame. The predicted size may be greater than the actual size
   * but it can never be less.
   *
   * @return predicted size
   */
  public int getPredictedSize()
  {
    int n = super.getPredictedSize();
    if (body == null)
    {
      n += 4; // For safety (length field and size of the list)
      n += codeConstructor.getPredictedSize();
      n += (handle != null?handle.getPredictedSize():1);
      n += (closed != null?closed.getPredictedSize():1);
      n += (error != null?error.getPredictedSize():1);
    } else
      n += body.getPredictedSize();
    return n;
  }

  private AMQPArray singleToArray(AMQPType t) throws IOException
  {
    return new AMQPArray(t.getCode(), new AMQPType[]{t});
  }

  private void decode() throws Exception
  {
    List l = body.getValue();
    AMQPType t = null;
    int idx = 0;


    // Field: handle
    // Type     : Handle, converted: Handle
    // Basetype : uint
    // Default  : null
    // Mandatory: true
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    if (t.getCode() == AMQPTypeDecoder.NULL)
      throw new Exception("Mandatory field 'handle' in 'Detach' frame is NULL");
    try {
      handle = new Handle(((AMQPUnsignedInt)t).getValue());
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'handle' in 'Detach' frame: "+e);
    }


    // Field: closed
    // Type     : boolean, converted: AMQPBoolean
    // Basetype : boolean
    // Default  : AMQPBoolean.FALSE
    // Mandatory: false
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    try {
      if (t.getCode() != AMQPTypeDecoder.NULL)
        closed = (AMQPBoolean)t;
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'closed' in 'Detach' frame: "+e);
    }


    // Field: error
    // Type     : Error, converted: Error
    // Basetype : list
    // Default  : null
    // Mandatory: false
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    try {
      if (t.getCode() != AMQPTypeDecoder.NULL)
        error = new Error(((AMQPList)t).getValue());
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'error' in 'Detach' frame: "+e);
    }
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
    addToList(l, handle);
    addToList(l, closed);
    addToList(l, error);
    for (ListIterator iter=l.listIterator(l.size());iter.hasPrevious();)
    {
        AMQPType t = (AMQPType)iter.previous();
        if (t.getCode() == AMQPTypeDecoder.NULL)
          iter.remove();
        else
          break;
    }
    body = new AMQPList(l);
    dirty = false;
  }

  protected void writeBody(DataOutput out) throws IOException
  {
    if (dirty || body == null)
      encode();
    codeConstructor.setFormatCode(body.getCode());
    body.setConstructor(codeConstructor);
    body.writeContent(out);
  }

  /**
   * Returns a value representation of this DetachFrame.
   *
   * @return value representation
   */
  public String getValueString()
  {
    try
    {
      if (dirty || body == null)
        encode();
    } catch (IOException e)
    {
      e.printStackTrace();
    }
    StringBuffer b = new StringBuffer("[Detach ");
    b.append(super.getValueString());
    b.append("]");
    return b.toString();
  }

  private String getDisplayString()
  {
    boolean _first = true;
    StringBuffer b = new StringBuffer();
    if (handle != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("handle=");
      b.append(handle.getValueString());
    }
    if (closed != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("closed=");
      b.append(closed.getValueString());
    }
    if (error != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("error=");
      b.append(error.getValueString());
    }
    return b.toString();
  }

  public String toString()
  {
    return "[Detach "+getDisplayString()+"]";
  }
}
