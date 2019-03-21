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
 * Updates the flow state for the specified link.
 * </p><p>
 * </p>
 *
 *  @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 **/

public class FlowFrame extends AMQPFrame
       implements FrameIF
{
  public static String DESCRIPTOR_NAME = "amqp:flow:list";
  public static long DESCRIPTOR_CODE = 0x00000000L<<32 | 0x00000013L;

  public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
  public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);
  
  AMQPList body = null;
  boolean dirty = false;

  TransferNumber nextIncomingId =  null;
  AMQPUnsignedInt incomingWindow =  null;
  TransferNumber nextOutgoingId =  null;
  AMQPUnsignedInt outgoingWindow =  null;
  Handle handle =  null;
  SequenceNo deliveryCount =  null;
  AMQPUnsignedInt linkCredit =  null;
  AMQPUnsignedInt available =  null;
  AMQPBoolean drain =  AMQPBoolean.FALSE;
  AMQPBoolean echo =  AMQPBoolean.FALSE;
  Fields properties =  null;

  /**
   * Constructs a FlowFrame.
   *
   * @param channel the channel id
   * @param body the frame body
   */
  public FlowFrame(int channel, AMQPList body) throws Exception
  {
    super(channel);
    this.body = body;
    if (body != null)
      decode();
  }

  /**
   * Constructs a FlowFrame.
   *
   * @param channel the channel id
   */
  public FlowFrame(int channel)
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
   * Sets the optional NextIncomingId field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * Identifies the expected transfer-id of the next incoming   frame.
   * This value MUST be set if the peer has received the   frame for the
   * session, and MUST NOT be set if it has not. See   for more details.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param nextIncomingId NextIncomingId
   */
  public void setNextIncomingId(TransferNumber nextIncomingId)
  {
    dirty = true;
    this.nextIncomingId = nextIncomingId;
  }

  /**
   * Returns the optional NextIncomingId field.
   *
   * @return NextIncomingId
   */
  public TransferNumber getNextIncomingId()
  {
    return nextIncomingId;
  }

  /**
   * Sets the mandatory IncomingWindow field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * Defines the maximum number of incoming   frames that the endpoint
   * can currently receive. See   for more
   * details.
   * </p><p>
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
   * Sets the mandatory NextOutgoingId field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * The transfer-id that will be assigned to the next outgoing
   * frame. See   for more details.
   * </p><p>
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
   * Sets the mandatory OutgoingWindow field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * Defines the maximum number of outgoing   frames that the endpoint
   * could potentially currently send, if it was not constrained by restrictions imposed by
   * its peer's incoming-window. See   for more
   * details.
   * </p><p>
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
   * Sets the optional Handle field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * If set, indicates that the flow frame carries flow state information for the local link
   * endpoint associated with the given handle. If not set, the flow frame is carrying only
   * information pertaining to the session endpoint.
   * </p><p>
   * </p><p>
   * If set to a handle that is not currently associated with an attached link, the
   * recipient MUST respond by ending the session with an   session error.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param handle Handle
   */
  public void setHandle(Handle handle)
  {
    dirty = true;
    this.handle = handle;
  }

  /**
   * Returns the optional Handle field.
   *
   * @return Handle
   */
  public Handle getHandle()
  {
    return handle;
  }

  /**
   * Sets the optional DeliveryCount field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * See   for the definition of delivery-count.
   * </p><p>
   * </p><p>
   * When the handle field is not set, this field MUST NOT be set.
   * </p><p>
   * </p><p>
   * When the handle identifies that the flow state is being sent from the sender link
   * endpoint to receiver link endpoint this field MUST be set to the current delivery-count
   * of the link endpoint.
   * </p><p>
   * </p><p>
   * When the flow state is being sent from the receiver endpoint to the sender endpoint this
   * field MUST be set to the last known value of the corresponding sending endpoint. In the
   * event that the receiving link endpoint has not yet  seen the initial
   * frame from the sender this field MUST NOT be set.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param deliveryCount DeliveryCount
   */
  public void setDeliveryCount(SequenceNo deliveryCount)
  {
    dirty = true;
    this.deliveryCount = deliveryCount;
  }

  /**
   * Returns the optional DeliveryCount field.
   *
   * @return DeliveryCount
   */
  public SequenceNo getDeliveryCount()
  {
    return deliveryCount;
  }

  /**
   * Sets the optional LinkCredit field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * The current maximum number of messages that can be handled at the receiver
   * endpoint of the link. Only the receiver endpoint can independently set this value. The
   * sender endpoint sets this to the last known value seen from the receiver. See
   * for more details.
   * </p><p>
   * </p><p>
   * When the handle field is not set, this field MUST NOT be set.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param linkCredit LinkCredit
   */
  public void setLinkCredit(AMQPUnsignedInt linkCredit)
  {
    dirty = true;
    this.linkCredit = linkCredit;
  }

  /**
   * Returns the optional LinkCredit field.
   *
   * @return LinkCredit
   */
  public AMQPUnsignedInt getLinkCredit()
  {
    return linkCredit;
  }

  /**
   * Sets the optional Available field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * The number of messages awaiting credit at the link sender endpoint. Only the
   * sender can independently set this value. The receiver sets this to the last known value
   * seen from the sender. See   for more details.
   * </p><p>
   * </p><p>
   * When the handle field is not set, this field MUST NOT be set.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param available Available
   */
  public void setAvailable(AMQPUnsignedInt available)
  {
    dirty = true;
    this.available = available;
  }

  /**
   * Returns the optional Available field.
   *
   * @return Available
   */
  public AMQPUnsignedInt getAvailable()
  {
    return available;
  }

  /**
   * Sets the optional Drain field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * When flow state is sent from the sender to the receiver, this field contains the actual
   * drain mode of the sender. When flow state is sent from the receiver to the sender, this
   * field contains the desired drain mode of the receiver. See   for more details.
   * </p><p>
   * </p><p>
   * When the handle field is not set, this field MUST NOT be set.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param drain Drain
   */
  public void setDrain(AMQPBoolean drain)
  {
    dirty = true;
    this.drain = drain;
  }

  /**
   * Returns the optional Drain field.
   *
   * @return Drain
   */
  public AMQPBoolean getDrain()
  {
    return drain;
  }

  /**
   * Sets the optional Echo field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * If set to true then the receiver should send its state at the earliest convenient
   * opportunity.
   * </p><p>
   * </p><p>
   * If set to true, and the handle field is not set, then the sender only requires session
   * endpoint state to be echoed, however, the receiver MAY fulfil this requirement by
   * sending a flow performative carrying link-specific state (since any such flow also
   * carries session state).
   * </p><p>
   * </p><p>
   * If a sender makes multiple requests for the same state before the receiver can reply,
   * the receiver MAY send only one flow in return.
   * </p><p>
   * </p><p>
   * Note that if a peer responds to echo requests with flows which themselves have the echo
   * field set to true, an infinite loop may result if its partner adopts the same policy
   * (therefore such a policy should be avoided).
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param echo Echo
   */
  public void setEcho(AMQPBoolean echo)
  {
    dirty = true;
    this.echo = echo;
  }

  /**
   * Returns the optional Echo field.
   *
   * @return Echo
   */
  public AMQPBoolean getEcho()
  {
    return echo;
  }

  /**
   * Sets the optional Properties field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * A registry of commonly defined link state properties and their meanings is maintained
   * [ AMQPLINKSTATEPROP ].
   * </p><p>
   * </p><p>
   * When the handle field is not set, this field MUST NOT be set.
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
   * Returns the predicted size of this FlowFrame. The predicted size may be greater than the actual size
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
      n += (nextIncomingId != null?nextIncomingId.getPredictedSize():1);
      n += (incomingWindow != null?incomingWindow.getPredictedSize():1);
      n += (nextOutgoingId != null?nextOutgoingId.getPredictedSize():1);
      n += (outgoingWindow != null?outgoingWindow.getPredictedSize():1);
      n += (handle != null?handle.getPredictedSize():1);
      n += (deliveryCount != null?deliveryCount.getPredictedSize():1);
      n += (linkCredit != null?linkCredit.getPredictedSize():1);
      n += (available != null?available.getPredictedSize():1);
      n += (drain != null?drain.getPredictedSize():1);
      n += (echo != null?echo.getPredictedSize():1);
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


    // Field: nextIncomingId
    // Type     : TransferNumber, converted: TransferNumber
    // Basetype : uint
    // Default  : null
    // Mandatory: false
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    try {
      if (t.getCode() != AMQPTypeDecoder.NULL)
        nextIncomingId = new TransferNumber(((AMQPUnsignedInt)t).getValue());
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'nextIncomingId' in 'Flow' frame: "+e);
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
      throw new Exception("Mandatory field 'incomingWindow' in 'Flow' frame is NULL");
    try {
      incomingWindow = (AMQPUnsignedInt)t;
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'incomingWindow' in 'Flow' frame: "+e);
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
      throw new Exception("Mandatory field 'nextOutgoingId' in 'Flow' frame is NULL");
    try {
      nextOutgoingId = new TransferNumber(((AMQPUnsignedInt)t).getValue());
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'nextOutgoingId' in 'Flow' frame: "+e);
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
      throw new Exception("Mandatory field 'outgoingWindow' in 'Flow' frame is NULL");
    try {
      outgoingWindow = (AMQPUnsignedInt)t;
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'outgoingWindow' in 'Flow' frame: "+e);
    }


    // Field: handle
    // Type     : Handle, converted: Handle
    // Basetype : uint
    // Default  : null
    // Mandatory: false
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    try {
      if (t.getCode() != AMQPTypeDecoder.NULL)
        handle = new Handle(((AMQPUnsignedInt)t).getValue());
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'handle' in 'Flow' frame: "+e);
    }


    // Field: deliveryCount
    // Type     : SequenceNo, converted: SequenceNo
    // Basetype : uint
    // Default  : null
    // Mandatory: false
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    try {
      if (t.getCode() != AMQPTypeDecoder.NULL)
        deliveryCount = new SequenceNo(((AMQPUnsignedInt)t).getValue());
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'deliveryCount' in 'Flow' frame: "+e);
    }


    // Field: linkCredit
    // Type     : uint, converted: AMQPUnsignedInt
    // Basetype : uint
    // Default  : null
    // Mandatory: false
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    try {
      if (t.getCode() != AMQPTypeDecoder.NULL)
        linkCredit = (AMQPUnsignedInt)t;
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'linkCredit' in 'Flow' frame: "+e);
    }


    // Field: available
    // Type     : uint, converted: AMQPUnsignedInt
    // Basetype : uint
    // Default  : null
    // Mandatory: false
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    try {
      if (t.getCode() != AMQPTypeDecoder.NULL)
        available = (AMQPUnsignedInt)t;
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'available' in 'Flow' frame: "+e);
    }


    // Field: drain
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
        drain = (AMQPBoolean)t;
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'drain' in 'Flow' frame: "+e);
    }


    // Field: echo
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
        echo = (AMQPBoolean)t;
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'echo' in 'Flow' frame: "+e);
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
      throw new Exception("Invalid type of field 'properties' in 'Flow' frame: "+e);
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
    addToList(l, nextIncomingId);
    addToList(l, incomingWindow);
    addToList(l, nextOutgoingId);
    addToList(l, outgoingWindow);
    addToList(l, handle);
    addToList(l, deliveryCount);
    addToList(l, linkCredit);
    addToList(l, available);
    addToList(l, drain);
    addToList(l, echo);
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
   * Returns a value representation of this FlowFrame.
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
    StringBuffer b = new StringBuffer("[Flow ");
    b.append(super.getValueString());
    b.append("]");
    return b.toString();
  }

  private String getDisplayString()
  {
    boolean _first = true;
    StringBuffer b = new StringBuffer();
    if (nextIncomingId != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("nextIncomingId=");
      b.append(nextIncomingId.getValueString());
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
    if (nextOutgoingId != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("nextOutgoingId=");
      b.append(nextOutgoingId.getValueString());
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
    if (handle != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("handle=");
      b.append(handle.getValueString());
    }
    if (deliveryCount != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("deliveryCount=");
      b.append(deliveryCount.getValueString());
    }
    if (linkCredit != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("linkCredit=");
      b.append(linkCredit.getValueString());
    }
    if (available != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("available=");
      b.append(available.getValueString());
    }
    if (drain != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("drain=");
      b.append(drain.getValueString());
    }
    if (echo != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("echo=");
      b.append(echo.getValueString());
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
    return "[Flow "+getDisplayString()+"]";
  }
}
