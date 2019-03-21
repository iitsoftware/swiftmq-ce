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
 * The   frame indicates that a link endpoint has been
 * attached to the session.
 * </p><p>
 * </p><p>
 * </p>
 *
 *  @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 **/

public class AttachFrame extends AMQPFrame
       implements FrameIF
{
  public static String DESCRIPTOR_NAME = "amqp:attach:list";
  public static long DESCRIPTOR_CODE = 0x00000000L<<32 | 0x00000012L;

  public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
  public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);
  
  AMQPList body = null;
  boolean dirty = false;

  AMQPString name =  null;
  Handle handle =  null;
  Role role =  null;
  SenderSettleMode sndSettleMode =  SenderSettleMode.MIXED;
  ReceiverSettleMode rcvSettleMode =  ReceiverSettleMode.FIRST;
  SourceIF source =  null;
  TargetIF target =  null;
  AMQPMap unsettled =  null;
  AMQPBoolean incompleteUnsettled =  AMQPBoolean.FALSE;
  SequenceNo initialDeliveryCount =  null;
  AMQPUnsignedLong maxMessageSize =  null;
  AMQPArray offeredCapabilities =  null;
  AMQPArray desiredCapabilities =  null;
  Fields properties =  null;

  /**
   * Constructs a AttachFrame.
   *
   * @param channel the channel id
   * @param body the frame body
   */
  public AttachFrame(int channel, AMQPList body) throws Exception
  {
    super(channel);
    this.body = body;
    if (body != null)
      decode();
  }

  /**
   * Constructs a AttachFrame.
   *
   * @param channel the channel id
   */
  public AttachFrame(int channel)
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
   * Sets the mandatory Name field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * This name uniquely identifies the link from the container of the source to the container
   * of the target node, e.g., if the container of the source node is A, and the container of
   * the target node is B, the link may be globally identified by the (ordered) tuple
   * (A,B, < name > ) .
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param name Name
   */
  public void setName(AMQPString name)
  {
    dirty = true;
    this.name = name;
  }

  /**
   * Returns the mandatory Name field.
   *
   * @return Name
   */
  public AMQPString getName()
  {
    return name;
  }

  /**
   * Sets the mandatory Handle field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * The numeric handle assigned by the the peer as a shorthand to refer to the link in all
   * performatives that reference the link until the it is detached. See  .
   * </p><p>
   * </p><p>
   * The handle MUST NOT be used for other open links. An attempt to attach using a handle
   * which is already associated with a link MUST be responded to with an immediate
   * carrying a handle-in-use  .
   * </p><p>
   * </p><p>
   * To make it easier to monitor AMQP link attach frames, it is recommended that
   * implementations always assign the lowest available handle to this field.
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
   * Returns the mandatory Handle field.
   *
   * @return Handle
   */
  public Handle getHandle()
  {
    return handle;
  }

  /**
   * Sets the mandatory Role field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * The role being played by the peer, i.e., whether the peer is the sender or the receiver
   * of messages on the link.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param role Role
   */
  public void setRole(Role role)
  {
    dirty = true;
    this.role = role;
  }

  /**
   * Returns the mandatory Role field.
   *
   * @return Role
   */
  public Role getRole()
  {
    return role;
  }

  /**
   * Sets the optional SndSettleMode field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * The delivery settlement policy for the sender. When set at the receiver this indicates
   * the desired value for the settlement mode at the sender. When set at the sender this
   * indicates the actual settlement mode in use. The sender SHOULD respect the receiver's
   * desired settlement mode if the receiver initiates the attach exchange and the sender
   * supports the desired mode.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param sndSettleMode SndSettleMode
   */
  public void setSndSettleMode(SenderSettleMode sndSettleMode)
  {
    dirty = true;
    this.sndSettleMode = sndSettleMode;
  }

  /**
   * Returns the optional SndSettleMode field.
   *
   * @return SndSettleMode
   */
  public SenderSettleMode getSndSettleMode()
  {
    return sndSettleMode;
  }

  /**
   * Sets the optional RcvSettleMode field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * The delivery settlement policy for the receiver. When set at the sender this indicates
   * the desired value for the settlement mode at the receiver. When set at the receiver this
   * indicates the actual settlement mode in use. The receiver SHOULD respect the sender's
   * desired settlement mode if the sender initiates the attach exchange and the receiver
   * supports the desired mode.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param rcvSettleMode RcvSettleMode
   */
  public void setRcvSettleMode(ReceiverSettleMode rcvSettleMode)
  {
    dirty = true;
    this.rcvSettleMode = rcvSettleMode;
  }

  /**
   * Returns the optional RcvSettleMode field.
   *
   * @return RcvSettleMode
   */
  public ReceiverSettleMode getRcvSettleMode()
  {
    return rcvSettleMode;
  }

  /**
   * Sets the optional Source field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * If no source is specified on an outgoing link, then there is no source currently
   * attached to the link. A link with no source will never produce outgoing messages.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param source Source
   */
  public void setSource(SourceIF source)
  {
    dirty = true;
    this.source = source;
  }

  /**
   * Returns the optional Source field.
   *
   * @return Source
   */
  public SourceIF getSource()
  {
    return source;
  }

  /**
   * Sets the optional Target field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * If no target is specified on an incoming link, then there is no target currently
   * attached to the link. A link with no target will never permit incoming messages.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param target Target
   */
  public void setTarget(TargetIF target)
  {
    dirty = true;
    this.target = target;
  }

  /**
   * Returns the optional Target field.
   *
   * @return Target
   */
  public TargetIF getTarget()
  {
    return target;
  }

  /**
   * Sets the optional Unsettled field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * This is used to indicate any unsettled delivery states when a suspended link is resumed.
   * The map is keyed by delivery-tag with values indicating the delivery state. The local
   * and remote delivery states for a given delivery-tag MUST be compared to resolve any
   * in-doubt deliveries. If necessary, deliveries MAY be resent, or resumed based on the
   * outcome of this comparison. See  .
   * </p><p>
   * </p><p>
   * If the local unsettled map is too large to be encoded within a frame of the agreed
   * maximum frame size then the session MAY be ended with the   error. The endpoint SHOULD make use of
   * the ability to send an incomplete unsettled map (see below) to avoid sending an error.
   * </p><p>
   * </p><p>
   * The unsettled map MUST NOT contain null valued keys.
   * </p><p>
   * </p><p>
   * When reattaching (as opposed to resuming), the unsettled map MUST be null.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param unsettled Unsettled
   */
  public void setUnsettled(AMQPMap unsettled)
  {
    dirty = true;
    this.unsettled = unsettled;
  }

  /**
   * Returns the optional Unsettled field.
   *
   * @return Unsettled
   */
  public AMQPMap getUnsettled()
  {
    return unsettled;
  }

  /**
   * Sets the optional IncompleteUnsettled field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * If set to true this field indicates that the unsettled map provided is not complete.
   * When the map is incomplete the recipient of the map cannot take the absence of a
   * delivery tag from the map as evidence of settlement. On receipt of an incomplete
   * unsettled map a sending endpoint MUST NOT send any new deliveries (i.e. deliveries where
   * resume is not set to true) to its partner (and a receiving endpoint which sent an
   * incomplete unsettled map MUST detach with an error on receiving a transfer which does
   * not have the resume flag set to true).
   * </p><p>
   * </p><p>
   * Note that if this flag is set to true then the endpoints MUST detach and reattach at
   * least once in order to send new deliveries. This flag can be useful when there are too
   * many entries in the unsettled map to fit within a single frame. An endpoint can attach,
   * resume, settle, and detach until enough unsettled state has been cleared for an attach
   * where this flag is set to false.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param incompleteUnsettled IncompleteUnsettled
   */
  public void setIncompleteUnsettled(AMQPBoolean incompleteUnsettled)
  {
    dirty = true;
    this.incompleteUnsettled = incompleteUnsettled;
  }

  /**
   * Returns the optional IncompleteUnsettled field.
   *
   * @return IncompleteUnsettled
   */
  public AMQPBoolean getIncompleteUnsettled()
  {
    return incompleteUnsettled;
  }

  /**
   * Sets the optional InitialDeliveryCount field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * This MUST NOT be null if role is sender, and it is ignored if the role is receiver. See
   * .
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param initialDeliveryCount InitialDeliveryCount
   */
  public void setInitialDeliveryCount(SequenceNo initialDeliveryCount)
  {
    dirty = true;
    this.initialDeliveryCount = initialDeliveryCount;
  }

  /**
   * Returns the optional InitialDeliveryCount field.
   *
   * @return InitialDeliveryCount
   */
  public SequenceNo getInitialDeliveryCount()
  {
    return initialDeliveryCount;
  }

  /**
   * Sets the optional MaxMessageSize field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * This field indicates the maximum message size supported by the link endpoint. Any
   * attempt to deliver a message larger than this results in a message-size-exceeded
   * . If this field is zero or unset, there is no maximum size
   * imposed by the link endpoint.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param maxMessageSize MaxMessageSize
   */
  public void setMaxMessageSize(AMQPUnsignedLong maxMessageSize)
  {
    dirty = true;
    this.maxMessageSize = maxMessageSize;
  }

  /**
   * Returns the optional MaxMessageSize field.
   *
   * @return MaxMessageSize
   */
  public AMQPUnsignedLong getMaxMessageSize()
  {
    return maxMessageSize;
  }

  /**
   * Sets the optional OfferedCapabilities field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * A registry of commonly defined link capabilities and their meanings is maintained
   * [ AMQPLINKCAP ].
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
   * The sender MUST NOT attempt to use any capability other than those it has declared in
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
   * link and its container.
   * </p><p>
   * </p><p>
   * A registry of commonly defined link properties and their meanings is maintained
   * [ AMQPLINKPROP ].
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
   * Returns the predicted size of this AttachFrame. The predicted size may be greater than the actual size
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
      n += (name != null?name.getPredictedSize():1);
      n += (handle != null?handle.getPredictedSize():1);
      n += (role != null?role.getPredictedSize():1);
      n += (sndSettleMode != null?sndSettleMode.getPredictedSize():1);
      n += (rcvSettleMode != null?rcvSettleMode.getPredictedSize():1);
      n += (source != null?source.getPredictedSize():1);
      n += (target != null?target.getPredictedSize():1);
      n += (unsettled != null?unsettled.getPredictedSize():1);
      n += (incompleteUnsettled != null?incompleteUnsettled.getPredictedSize():1);
      n += (initialDeliveryCount != null?initialDeliveryCount.getPredictedSize():1);
      n += (maxMessageSize != null?maxMessageSize.getPredictedSize():1);
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


    // Field: name
    // Type     : string, converted: AMQPString
    // Basetype : string
    // Default  : null
    // Mandatory: true
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    if (t.getCode() == AMQPTypeDecoder.NULL)
      throw new Exception("Mandatory field 'name' in 'Attach' frame is NULL");
    try {
      name = (AMQPString)t;
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'name' in 'Attach' frame: "+e);
    }


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
      throw new Exception("Mandatory field 'handle' in 'Attach' frame is NULL");
    try {
      handle = new Handle(((AMQPUnsignedInt)t).getValue());
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'handle' in 'Attach' frame: "+e);
    }


    // Field: role
    // Type     : Role, converted: Role
    // Basetype : boolean
    // Default  : null
    // Mandatory: true
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    if (t.getCode() == AMQPTypeDecoder.NULL)
      throw new Exception("Mandatory field 'role' in 'Attach' frame is NULL");
    try {
      role = new Role(((AMQPBoolean)t).getValue());
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'role' in 'Attach' frame: "+e);
    }


    // Field: sndSettleMode
    // Type     : SenderSettleMode, converted: SenderSettleMode
    // Basetype : ubyte
    // Default  : SenderSettleMode.MIXED
    // Mandatory: false
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    try {
      if (t.getCode() != AMQPTypeDecoder.NULL)
        sndSettleMode = new SenderSettleMode(((AMQPUnsignedByte)t).getValue());
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'sndSettleMode' in 'Attach' frame: "+e);
    }


    // Field: rcvSettleMode
    // Type     : ReceiverSettleMode, converted: ReceiverSettleMode
    // Basetype : ubyte
    // Default  : ReceiverSettleMode.FIRST
    // Mandatory: false
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    try {
      if (t.getCode() != AMQPTypeDecoder.NULL)
        rcvSettleMode = new ReceiverSettleMode(((AMQPUnsignedByte)t).getValue());
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'rcvSettleMode' in 'Attach' frame: "+e);
    }


    // Field: source
    // Type     : SourceIF, converted: SourceIF
    // Basetype : SourceIF
    // Default  : null
    // Mandatory: false
    // Multiple : false
    // Factory  : SourceFactory
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    if (t.getCode() != AMQPTypeDecoder.NULL)
      source = SourceFactory.create(t);


    // Field: target
    // Type     : TargetIF, converted: TargetIF
    // Basetype : TargetIF
    // Default  : null
    // Mandatory: false
    // Multiple : false
    // Factory  : TargetFactory
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    if (t.getCode() != AMQPTypeDecoder.NULL)
      target = TargetFactory.create(t);


    // Field: unsettled
    // Type     : map, converted: AMQPMap
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
        unsettled = (AMQPMap)t;
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'unsettled' in 'Attach' frame: "+e);
    }


    // Field: incompleteUnsettled
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
        incompleteUnsettled = (AMQPBoolean)t;
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'incompleteUnsettled' in 'Attach' frame: "+e);
    }


    // Field: initialDeliveryCount
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
        initialDeliveryCount = new SequenceNo(((AMQPUnsignedInt)t).getValue());
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'initialDeliveryCount' in 'Attach' frame: "+e);
    }


    // Field: maxMessageSize
    // Type     : ulong, converted: AMQPUnsignedLong
    // Basetype : ulong
    // Default  : null
    // Mandatory: false
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    try {
      if (t.getCode() != AMQPTypeDecoder.NULL)
        maxMessageSize = (AMQPUnsignedLong)t;
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'maxMessageSize' in 'Attach' frame: "+e);
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
      throw new Exception("Invalid type of field 'offeredCapabilities' in 'Attach' frame: "+e);
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
      throw new Exception("Invalid type of field 'desiredCapabilities' in 'Attach' frame: "+e);
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
      throw new Exception("Invalid type of field 'properties' in 'Attach' frame: "+e);
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
    addToList(l, name);
    addToList(l, handle);
    addToList(l, role);
    addToList(l, sndSettleMode);
    addToList(l, rcvSettleMode);
    addToList(l, source);
    addToList(l, target);
    addToList(l, unsettled);
    addToList(l, incompleteUnsettled);
    addToList(l, initialDeliveryCount);
    addToList(l, maxMessageSize);
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
   * Returns a value representation of this AttachFrame.
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
    StringBuffer b = new StringBuffer("[Attach ");
    b.append(super.getValueString());
    b.append("]");
    return b.toString();
  }

  private String getDisplayString()
  {
    boolean _first = true;
    StringBuffer b = new StringBuffer();
    if (name != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("name=");
      b.append(name.getValueString());
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
    if (role != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("role=");
      b.append(role.getValueString());
    }
    if (sndSettleMode != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("sndSettleMode=");
      b.append(sndSettleMode.getValueString());
    }
    if (rcvSettleMode != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("rcvSettleMode=");
      b.append(rcvSettleMode.getValueString());
    }
    if (source != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("source=");
      b.append(source.getValueString());
    }
    if (target != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("target=");
      b.append(target.getValueString());
    }
    if (unsettled != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("unsettled=");
      b.append(unsettled.getValueString());
    }
    if (incompleteUnsettled != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("incompleteUnsettled=");
      b.append(incompleteUnsettled.getValueString());
    }
    if (initialDeliveryCount != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("initialDeliveryCount=");
      b.append(initialDeliveryCount.getValueString());
    }
    if (maxMessageSize != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("maxMessageSize=");
      b.append(maxMessageSize.getValueString());
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
    return "[Attach "+getDisplayString()+"]";
  }
}
