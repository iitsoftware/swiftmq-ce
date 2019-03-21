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
 * Indicate that a session has begun on the channel.
 * </p><p>
 * </p><p>
 * </p>
 *
 *  @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 **/

public class BeginFrame extends AMQPFrame
       implements FrameIF
{
  public static String DESCRIPTOR_NAME = "amqp:begin:list";
  public static long DESCRIPTOR_CODE = 0x00000000L<<32 | 0x00000011L;

  public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
  public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);
  
  AMQPList body = null;
  boolean dirty = false;

  AMQPUnsignedShort remoteChannel =  null;
  TransferNumber nextOutgoingId =  null;
  AMQPUnsignedInt incomingWindow =  null;
  AMQPUnsignedInt outgoingWindow =  null;
  Handle handleMax = new Handle(4294967295L);
  AMQPArray offeredCapabilities =  null;
  AMQPArray desiredCapabilities =  null;
  Fields properties =  null;

  /**
   * Constructs a BeginFrame.
   *
   * @param channel the channel id
   * @param body the frame body
   */
  public BeginFrame(int channel, AMQPList body) throws Exception
  {
    super(channel);
    this.body = body;
    if (body != null)
      decode();
  }

  /**
   * Constructs a BeginFrame.
   *
   * @param channel the channel id
   */
  public BeginFrame(int channel)
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
   * Sets the optional RemoteChannel field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * If a session is locally initiated, the remote-channel MUST NOT be set. When an endpoint
   * responds to a remotely initiated session, the remote-channel MUST be set to the channel
   * on which the remote session sent the begin.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param remoteChannel RemoteChannel
   */
  public void setRemoteChannel(AMQPUnsignedShort remoteChannel)
  {
    dirty = true;
    this.remoteChannel = remoteChannel;
  }

  /**
   * Returns the optional RemoteChannel field.
   *
   * @return RemoteChannel
   */
  public AMQPUnsignedShort getRemoteChannel()
  {
    return remoteChannel;
  }

  /**
   * Sets the mandatory NextOutgoingId field.
   *
   * <p>
   * </p><p>
   * See  .
   * </p><p>
   * </p><p>
   * </p>
   * @param nextOutgoingId NextOutgoingId
   */
  public void setNextOutgoingId(TransferNumber nextOutgoingId)
  {
    dirty = true;
    this.nextOutgoingId = nextOutgoingId;
  }

  /**
   * Returns the mandatory NextOutgoingId field.
   *
   * @return NextOutgoingId
   */
  public TransferNumber getNextOutgoingId()
  {
    return nextOutgoingId;
  }

  /**
   * Sets the mandatory IncomingWindow field.
   *
   * <p>
   * </p><p>
   * See  .
   * </p><p>
   * </p><p>
   * </p>
   * @param incomingWindow IncomingWindow
   */
  public void setIncomingWindow(AMQPUnsignedInt incomingWindow)
  {
    dirty = true;
    this.incomingWindow = incomingWindow;
  }

  /**
   * Returns the mandatory IncomingWindow field.
   *
   * @return IncomingWindow
   */
  public AMQPUnsignedInt getIncomingWindow()
  {
    return incomingWindow;
  }

  /**
   * Sets the mandatory OutgoingWindow field.
   *
   * <p>
   * </p><p>
   * See  .
   * </p><p>
   * </p><p>
   * </p>
   * @param outgoingWindow OutgoingWindow
   */
  public void setOutgoingWindow(AMQPUnsignedInt outgoingWindow)
  {
    dirty = true;
    this.outgoingWindow = outgoingWindow;
  }

  /**
   * Returns the mandatory OutgoingWindow field.
   *
   * @return OutgoingWindow
   */
  public AMQPUnsignedInt getOutgoingWindow()
  {
    return outgoingWindow;
  }

  /**
   * Sets the optional HandleMax field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * The handle-max value is the highest handle value that may be used on the session.
   * A peer MUST NOT attempt to attach a link using a handle value outside the range that its
   * partner can handle. A peer that receives a handle outside the supported range MUST close
   * the connection with the framing-error error-code.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param handleMax HandleMax
   */
  public void setHandleMax(Handle handleMax)
  {
    dirty = true;
    this.handleMax = handleMax;
  }

  /**
   * Returns the optional HandleMax field.
   *
   * @return HandleMax
   */
  public Handle getHandleMax()
  {
    return handleMax;
  }

  /**
   * Sets the optional OfferedCapabilities field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * A registry of commonly defined session capabilities and their meanings is maintained
   * [ AMQPSESSCAP ].
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param offeredCapabilities OfferedCapabilities
   */
  public void setOfferedCapabilities(AMQPArray offeredCapabilities)
  {
    dirty = true;
    this.offeredCapabilities = offeredCapabilities;
  }

  /**
   * Returns the optional OfferedCapabilities field.
   *
   * @return OfferedCapabilities
   */
  public AMQPArray getOfferedCapabilities()
  {
    return offeredCapabilities;
  }

  /**
   * Sets the optional DesiredCapabilities field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * The sender MUST NOT attempt to use any capability other than those it has decl ared in
   * desired-capabilities field.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param desiredCapabilities DesiredCapabilities
   */
  public void setDesiredCapabilities(AMQPArray desiredCapabilities)
  {
    dirty = true;
    this.desiredCapabilities = desiredCapabilities;
  }

  /**
   * Returns the optional DesiredCapabilities field.
   *
   * @return DesiredCapabilities
   */
  public AMQPArray getDesiredCapabilities()
  {
    return desiredCapabilities;
  }

  /**
   * Sets the optional Properties field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * The properties map contains a set of fields intended to indicate information about the
   * session and its container.
   * </p><p>
   * </p><p>
   * A registry of commonly defined session properties and their meanings is maintained
   * [ AMQPSESSPROP ].
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param properties Properties
   */
  public void setProperties(Fields properties)
  {
    dirty = true;
    this.properties = properties;
  }

  /**
   * Returns the optional Properties field.
   *
   * @return Properties
   */
  public Fields getProperties()
  {
    return properties;
  }


  /**
   * Returns the predicted size of this BeginFrame. The predicted size may be greater than the actual size
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
      n += (remoteChannel != null?remoteChannel.getPredictedSize():1);
      n += (nextOutgoingId != null?nextOutgoingId.getPredictedSize():1);
      n += (incomingWindow != null?incomingWindow.getPredictedSize():1);
      n += (outgoingWindow != null?outgoingWindow.getPredictedSize():1);
      n += (handleMax != null?handleMax.getPredictedSize():1);
      n += (offeredCapabilities != null?offeredCapabilities.getPredictedSize():1);
      n += (desiredCapabilities != null?desiredCapabilities.getPredictedSize():1);
      n += (properties != null?properties.getPredictedSize():1);
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


    // Field: remoteChannel
    // Type     : ushort, converted: AMQPUnsignedShort
    // Basetype : ushort
    // Default  : null
    // Mandatory: false
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    try {
      if (t.getCode() != AMQPTypeDecoder.NULL)
        remoteChannel = (AMQPUnsignedShort)t;
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'remoteChannel' in 'Begin' frame: "+e);
    }


    // Field: nextOutgoingId
    // Type     : TransferNumber, converted: TransferNumber
    // Basetype : uint
    // Default  : null
    // Mandatory: true
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    if (t.getCode() == AMQPTypeDecoder.NULL)
      throw new Exception("Mandatory field 'nextOutgoingId' in 'Begin' frame is NULL");
    try {
      nextOutgoingId = new TransferNumber(((AMQPUnsignedInt)t).getValue());
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'nextOutgoingId' in 'Begin' frame: "+e);
    }


    // Field: incomingWindow
    // Type     : uint, converted: AMQPUnsignedInt
    // Basetype : uint
    // Default  : null
    // Mandatory: true
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    if (t.getCode() == AMQPTypeDecoder.NULL)
      throw new Exception("Mandatory field 'incomingWindow' in 'Begin' frame is NULL");
    try {
      incomingWindow = (AMQPUnsignedInt)t;
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'incomingWindow' in 'Begin' frame: "+e);
    }


    // Field: outgoingWindow
    // Type     : uint, converted: AMQPUnsignedInt
    // Basetype : uint
    // Default  : null
    // Mandatory: true
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    if (t.getCode() == AMQPTypeDecoder.NULL)
      throw new Exception("Mandatory field 'outgoingWindow' in 'Begin' frame is NULL");
    try {
      outgoingWindow = (AMQPUnsignedInt)t;
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'outgoingWindow' in 'Begin' frame: "+e);
    }


    // Field: handleMax
    // Type     : Handle, converted: Handle
    // Basetype : uint
    // Default  : 4294967295
    // Mandatory: false
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    try {
      if (t.getCode() != AMQPTypeDecoder.NULL)
        handleMax = new Handle(((AMQPUnsignedInt)t).getValue());
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'handleMax' in 'Begin' frame: "+e);
    }


    // Field: offeredCapabilities
    // Type     : symbol, converted: AMQPSymbol
    // Basetype : symbol
    // Default  : null
    // Mandatory: false
    // Multiple : true
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    try {
      if (t.getCode() != AMQPTypeDecoder.NULL)
        offeredCapabilities = AMQPTypeDecoder.isArray(t.getCode())?(AMQPArray)t:singleToArray(t);
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'offeredCapabilities' in 'Begin' frame: "+e);
    }


    // Field: desiredCapabilities
    // Type     : symbol, converted: AMQPSymbol
    // Basetype : symbol
    // Default  : null
    // Mandatory: false
    // Multiple : true
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    try {
      if (t.getCode() != AMQPTypeDecoder.NULL)
        desiredCapabilities = AMQPTypeDecoder.isArray(t.getCode())?(AMQPArray)t:singleToArray(t);
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'desiredCapabilities' in 'Begin' frame: "+e);
    }


    // Field: properties
    // Type     : Fields, converted: Fields
    // Basetype : map
    // Default  : null
    // Mandatory: false
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    try {
      if (t.getCode() != AMQPTypeDecoder.NULL)
        properties = new Fields(((AMQPMap)t).getValue());
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'properties' in 'Begin' frame: "+e);
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
    addToList(l, remoteChannel);
    addToList(l, nextOutgoingId);
    addToList(l, incomingWindow);
    addToList(l, outgoingWindow);
    addToList(l, handleMax);
    addToList(l, offeredCapabilities);
    addToList(l, desiredCapabilities);
    addToList(l, properties);
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
   * Returns a value representation of this BeginFrame.
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
    StringBuffer b = new StringBuffer("[Begin ");
    b.append(super.getValueString());
    b.append("]");
    return b.toString();
  }

  private String getDisplayString()
  {
    boolean _first = true;
    StringBuffer b = new StringBuffer();
    if (remoteChannel != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("remoteChannel=");
      b.append(remoteChannel.getValueString());
    }
    if (nextOutgoingId != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("nextOutgoingId=");
      b.append(nextOutgoingId.getValueString());
    }
    if (incomingWindow != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("incomingWindow=");
      b.append(incomingWindow.getValueString());
    }
    if (outgoingWindow != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("outgoingWindow=");
      b.append(outgoingWindow.getValueString());
    }
    if (handleMax != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("handleMax=");
      b.append(handleMax.getValueString());
    }
    if (offeredCapabilities != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("offeredCapabilities=");
      b.append(offeredCapabilities.getValueString());
    }
    if (desiredCapabilities != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("desiredCapabilities=");
      b.append(desiredCapabilities.getValueString());
    }
    if (properties != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("properties=");
      b.append(properties.getValueString());
    }
    return b.toString();
  }

  public String toString()
  {
    return "[Begin "+getDisplayString()+"]";
  }
}
