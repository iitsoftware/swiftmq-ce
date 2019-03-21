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
 * The first frame sent on a connection in either direction MUST contain an open
 * performative. Note that the connection header which is sent first on the connection is
 * not  a frame.
 * </p><p>
 * </p><p>
 * The fields indicate the capabilities and limitations of the sending peer.
 * </p><p>
 * </p><p>
 * </p>
 *
 *  @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 **/

public class OpenFrame extends AMQPFrame
       implements FrameIF
{
  public static String DESCRIPTOR_NAME = "amqp:open:list";
  public static long DESCRIPTOR_CODE = 0x00000000L<<32 | 0x00000010L;

  public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
  public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);
  
  AMQPList body = null;
  boolean dirty = false;

  AMQPString containerId =  null;
  AMQPString hostname =  null;
  AMQPUnsignedInt maxFrameSize = new AMQPUnsignedInt(4294967295L);
  AMQPUnsignedShort channelMax = new AMQPUnsignedShort(65535);
  Milliseconds idleTimeOut =  null;
  AMQPArray outgoingLocales =  null;
  AMQPArray incomingLocales =  null;
  AMQPArray offeredCapabilities =  null;
  AMQPArray desiredCapabilities =  null;
  Fields properties =  null;

  /**
   * Constructs a OpenFrame.
   *
   * @param channel the channel id
   * @param body the frame body
   */
  public OpenFrame(int channel, AMQPList body) throws Exception
  {
    super(channel);
    this.body = body;
    if (body != null)
      decode();
  }

  /**
   * Constructs a OpenFrame.
   *
   * @param channel the channel id
   */
  public OpenFrame(int channel)
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
   * Sets the mandatory ContainerId field.
   *
   * <p>
   * </p>
   * @param containerId ContainerId
   */
  public void setContainerId(AMQPString containerId)
  {
    dirty = true;
    this.containerId = containerId;
  }

  /**
   * Returns the mandatory ContainerId field.
   *
   * @return ContainerId
   */
  public AMQPString getContainerId()
  {
    return containerId;
  }

  /**
   * Sets the optional Hostname field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * The name of the host (either fully qualified or relative) to which the sending peer
   * is connecting. It is not mandatory to provide the hostname. If no hostname is provided
   * the receiving peer should select a default based on its own configuration. This field
   * can be used by AMQP proxies to determine the correct back-end service to connect
   * the client to.
   * </p><p>
   * </p><p>
   * This field may already have been specified by the   frame, if a
   * SASL layer is used, or, the server name indication extension as described in
   * RFC-4366, if a TLS layer is used, in which case this field SHOULD be null or contain
   * the same value. It is undefined what a different value to that already specified
   * means.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param hostname Hostname
   */
  public void setHostname(AMQPString hostname)
  {
    dirty = true;
    this.hostname = hostname;
  }

  /**
   * Returns the optional Hostname field.
   *
   * @return Hostname
   */
  public AMQPString getHostname()
  {
    return hostname;
  }

  /**
   * Sets the optional MaxFrameSize field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * The largest frame size that the sending peer is able to accept on this connection. If
   * this field is not set it means that the peer does not impose any specific limit. A peer
   * MUST NOT send frames larger than its partner can handle. A peer that receives an
   * oversized frame MUST close the connection with the framing-error error-code.
   * </p><p>
   * </p><p>
   * Both peers MUST accept frames of up to   octets.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param maxFrameSize MaxFrameSize
   */
  public void setMaxFrameSize(AMQPUnsignedInt maxFrameSize)
  {
    dirty = true;
    this.maxFrameSize = maxFrameSize;
  }

  /**
   * Returns the optional MaxFrameSize field.
   *
   * @return MaxFrameSize
   */
  public AMQPUnsignedInt getMaxFrameSize()
  {
    return maxFrameSize;
  }

  /**
   * Sets the optional ChannelMax field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * The channel-max value is the highest channel number that may be used on the connection.
   * This value plus one is the maximum number of sessions that can be simultaneously active
   * on the connection. A peer MUST not use channel numbers outside the range that its
   * partner can handle. A peer that receives a channel number outside the supported range
   * MUST close the connection with the framing-error error-code.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param channelMax ChannelMax
   */
  public void setChannelMax(AMQPUnsignedShort channelMax)
  {
    dirty = true;
    this.channelMax = channelMax;
  }

  /**
   * Returns the optional ChannelMax field.
   *
   * @return ChannelMax
   */
  public AMQPUnsignedShort getChannelMax()
  {
    return channelMax;
  }

  /**
   * Sets the optional IdleTimeOut field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * The idle timeout required by the sender (see  ). A value
   * of zero is the same as if it was not set (null). If the receiver is unable or unwilling
   * to support the idle time-out then it should close the connection with an error
   * explaining why (e.g., because it is too small).
   * </p><p>
   * </p><p>
   * If the value is not set, then the sender does not have an idle time-out. However,
   * senders doing this should be aware that implementations MAY choose to use an
   * internal default to efficiently manage a peer's resources.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param idleTimeOut IdleTimeOut
   */
  public void setIdleTimeOut(Milliseconds idleTimeOut)
  {
    dirty = true;
    this.idleTimeOut = idleTimeOut;
  }

  /**
   * Returns the optional IdleTimeOut field.
   *
   * @return IdleTimeOut
   */
  public Milliseconds getIdleTimeOut()
  {
    return idleTimeOut;
  }

  /**
   * Sets the optional OutgoingLocales field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * A list of the locales that the peer supports for sending informational text. This
   * includes connection, session and link error descriptions. A peer MUST support at least
   * the  en-US  locale (see  ). Since this value is
   * always supported, it need not be supplied in the outgoing-locales. A null value or an
   * empty list implies that only  en-US  is supported.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param outgoingLocales OutgoingLocales
   */
  public void setOutgoingLocales(AMQPArray outgoingLocales)
  {
    dirty = true;
    this.outgoingLocales = outgoingLocales;
  }

  /**
   * Returns the optional OutgoingLocales field.
   *
   * @return OutgoingLocales
   */
  public AMQPArray getOutgoingLocales()
  {
    return outgoingLocales;
  }

  /**
   * Sets the optional IncomingLocales field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * A list of locales that the sending peer permits for incoming informational text. This
   * list is ordered in decreasing level of preference. The receiving partner will choose the
   * first (most preferred) incoming locale from those which it supports. If none of the
   * requested locales are supported,  en-US  will be chosen. Note that  en-US
   * need not be supplied in this list as it is always the fallback. A peer may determine
   * which of the permitted incoming locales is chosen by examining the partner's supported
   * locales as specified in the outgoing-locales field. A null value or an empty list
   * implies that only  en-US  is supported.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param incomingLocales IncomingLocales
   */
  public void setIncomingLocales(AMQPArray incomingLocales)
  {
    dirty = true;
    this.incomingLocales = incomingLocales;
  }

  /**
   * Returns the optional IncomingLocales field.
   *
   * @return IncomingLocales
   */
  public AMQPArray getIncomingLocales()
  {
    return incomingLocales;
  }

  /**
   * Sets the optional OfferedCapabilities field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * If the receiver of the offered-capabilities requires an extension capability which is
   * not present in the offered-capability list then it MUST close the connection.
   * </p><p>
   * </p><p>
   * A registry of commonly defined connection capabilities and their meanings is maintained
   * [ AMQPCONNCAP ].
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
   * The desired-capability list defines which extension capabilities the sender MAY use if
   * the receiver offers them (i.e., they are in the offered-capabilities list received by
   * the sender of the desired-capabilities). The sender MUST NOT attempt to use any
   * capabilities it did not declare in the desired-capabilities field. If the receiver of
   * the desired-capabilities offers extension capabilities which are not present in the
   * desired-capabilities list it received, then it can be sure those (undesired)
   * capabilities will not be used on the connection.
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
   * connection and its container.
   * </p><p>
   * </p><p>
   * A registry of commonly defined connection properties and their meanings is maintained
   * [ AMQPCONNPROP ].
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
   * Returns the predicted size of this OpenFrame. The predicted size may be greater than the actual size
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
      n += (containerId != null?containerId.getPredictedSize():1);
      n += (hostname != null?hostname.getPredictedSize():1);
      n += (maxFrameSize != null?maxFrameSize.getPredictedSize():1);
      n += (channelMax != null?channelMax.getPredictedSize():1);
      n += (idleTimeOut != null?idleTimeOut.getPredictedSize():1);
      n += (outgoingLocales != null?outgoingLocales.getPredictedSize():1);
      n += (incomingLocales != null?incomingLocales.getPredictedSize():1);
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


    // Field: containerId
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
      throw new Exception("Mandatory field 'containerId' in 'Open' frame is NULL");
    try {
      containerId = (AMQPString)t;
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'containerId' in 'Open' frame: "+e);
    }


    // Field: hostname
    // Type     : string, converted: AMQPString
    // Basetype : string
    // Default  : null
    // Mandatory: false
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    try {
      if (t.getCode() != AMQPTypeDecoder.NULL)
        hostname = (AMQPString)t;
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'hostname' in 'Open' frame: "+e);
    }


    // Field: maxFrameSize
    // Type     : uint, converted: AMQPUnsignedInt
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
        maxFrameSize = (AMQPUnsignedInt)t;
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'maxFrameSize' in 'Open' frame: "+e);
    }


    // Field: channelMax
    // Type     : ushort, converted: AMQPUnsignedShort
    // Basetype : ushort
    // Default  : 65535
    // Mandatory: false
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    try {
      if (t.getCode() != AMQPTypeDecoder.NULL)
        channelMax = (AMQPUnsignedShort)t;
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'channelMax' in 'Open' frame: "+e);
    }


    // Field: idleTimeOut
    // Type     : Milliseconds, converted: Milliseconds
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
        idleTimeOut = new Milliseconds(((AMQPUnsignedInt)t).getValue());
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'idleTimeOut' in 'Open' frame: "+e);
    }


    // Field: outgoingLocales
    // Type     : IetfLanguageTag, converted: IetfLanguageTag
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
        outgoingLocales = AMQPTypeDecoder.isArray(t.getCode())?(AMQPArray)t:singleToArray(t);
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'outgoingLocales' in 'Open' frame: "+e);
    }


    // Field: incomingLocales
    // Type     : IetfLanguageTag, converted: IetfLanguageTag
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
        incomingLocales = AMQPTypeDecoder.isArray(t.getCode())?(AMQPArray)t:singleToArray(t);
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'incomingLocales' in 'Open' frame: "+e);
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
      throw new Exception("Invalid type of field 'offeredCapabilities' in 'Open' frame: "+e);
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
      throw new Exception("Invalid type of field 'desiredCapabilities' in 'Open' frame: "+e);
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
      throw new Exception("Invalid type of field 'properties' in 'Open' frame: "+e);
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
    addToList(l, containerId);
    addToList(l, hostname);
    addToList(l, maxFrameSize);
    addToList(l, channelMax);
    addToList(l, idleTimeOut);
    addToList(l, outgoingLocales);
    addToList(l, incomingLocales);
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
   * Returns a value representation of this OpenFrame.
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
    StringBuffer b = new StringBuffer("[Open ");
    b.append(super.getValueString());
    b.append("]");
    return b.toString();
  }

  private String getDisplayString()
  {
    boolean _first = true;
    StringBuffer b = new StringBuffer();
    if (containerId != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("containerId=");
      b.append(containerId.getValueString());
    }
    if (hostname != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("hostname=");
      b.append(hostname.getValueString());
    }
    if (maxFrameSize != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("maxFrameSize=");
      b.append(maxFrameSize.getValueString());
    }
    if (channelMax != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("channelMax=");
      b.append(channelMax.getValueString());
    }
    if (idleTimeOut != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("idleTimeOut=");
      b.append(idleTimeOut.getValueString());
    }
    if (outgoingLocales != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("outgoingLocales=");
      b.append(outgoingLocales.getValueString());
    }
    if (incomingLocales != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("incomingLocales=");
      b.append(incomingLocales.getValueString());
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
    return "[Open "+getDisplayString()+"]";
  }
}
