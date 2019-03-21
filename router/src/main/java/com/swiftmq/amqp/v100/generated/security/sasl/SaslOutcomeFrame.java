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

package com.swiftmq.amqp.v100.generated.security.sasl;

import com.swiftmq.amqp.v100.types.*;
import com.swiftmq.amqp.v100.transport.*;
import com.swiftmq.amqp.v100.generated.*;
import com.swiftmq.amqp.v100.generated.transport.definitions.Error;
import com.swiftmq.amqp.v100.generated.transport.performatives.*;
import com.swiftmq.amqp.v100.generated.transport.definitions.*;
import com.swiftmq.amqp.v100.generated.messaging.message_format.*;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.*;
import com.swiftmq.amqp.v100.generated.messaging.addressing.*;
import com.swiftmq.amqp.v100.generated.transactions.coordination.*;
import com.swiftmq.amqp.v100.generated.provides.global_tx_id_types.*;
import com.swiftmq.amqp.v100.generated.filter.filter_types.*;
import java.io.*;
import java.util.*;

/**
 * <p>
 * </p><p>
 * This frame indicates the outcome of the SASL dialog. Upon successful completion of the
 * SASL dialog the security layer has been established, and the peers must exchange protocol
 * headers to either start a nested security layer, or to establish the AMQP connection.
 * </p><p>
 * </p><p>
 * </p>
 *
 *  @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 **/

public class SaslOutcomeFrame extends AMQPFrame
       implements SaslFrameIF
{
  public static String DESCRIPTOR_NAME = "amqp:sasl-outcome:list";
  public static long DESCRIPTOR_CODE = 0x00000000L<<32 | 0x00000044L;

  public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
  public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);
  
  AMQPList body = null;
  boolean dirty = false;

  SaslCode mycode =  null;
  AMQPBinary additionalData =  null;

  /**
   * Constructs a SaslOutcomeFrame.
   *
   * @param channel the channel id
   * @param body the frame body
   */
  public SaslOutcomeFrame(int channel, AMQPList body) throws Exception
  {
    super(channel);
    setTypeCode(TYPE_CODE_SASL_FRAME);
    this.body = body;
    if (body != null)
      decode();
  }

  /**
   * Constructs a SaslOutcomeFrame.
   *
   * @param channel the channel id
   */
  public SaslOutcomeFrame(int channel)
  {
    super(channel);
    setTypeCode(TYPE_CODE_SASL_FRAME);
  }

  /**
   * Accept method for a SaslFrame visitor.
   *
   * @param visitor SaslFrame visitor
   */
  public void accept(SaslFrameVisitor visitor)
  {
    visitor.visit(this);
  }


  /**
   * Sets the mandatory Mycode field.
   *
   * <p>
   * </p><p>
   * A reply-code indicating the outcome of the SASL dialog.
   * </p><p>
   * </p><p>
   * </p>
   * @param mycode Mycode
   */
  public void setMycode(SaslCode mycode)
  {
    dirty = true;
    this.mycode = mycode;
  }

  /**
   * Returns the mandatory Mycode field.
   *
   * @return Mycode
   */
  public SaslCode getMycode()
  {
    return mycode;
  }

  /**
   * Sets the optional AdditionalData field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * The additional-data field carries additional data on successful authentication outcome
   * as specified by the SASL specification [ RFC4422 ].
   * If the authentication is unsuccessful, this field is not set.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param additionalData AdditionalData
   */
  public void setAdditionalData(AMQPBinary additionalData)
  {
    dirty = true;
    this.additionalData = additionalData;
  }

  /**
   * Returns the optional AdditionalData field.
   *
   * @return AdditionalData
   */
  public AMQPBinary getAdditionalData()
  {
    return additionalData;
  }


  /**
   * Returns the predicted size of this SaslOutcomeFrame. The predicted size may be greater than the actual size
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
      n += (mycode != null?mycode.getPredictedSize():1);
      n += (additionalData != null?additionalData.getPredictedSize():1);
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


    // Field: mycode
    // Type     : SaslCode, converted: SaslCode
    // Basetype : ubyte
    // Default  : null
    // Mandatory: true
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    if (t.getCode() == AMQPTypeDecoder.NULL)
      throw new Exception("Mandatory field 'mycode' in 'SaslOutcome' frame is NULL");
    try {
      mycode = new SaslCode(((AMQPUnsignedByte)t).getValue());
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'mycode' in 'SaslOutcome' frame: "+e);
    }


    // Field: additionalData
    // Type     : binary, converted: AMQPBinary
    // Basetype : binary
    // Default  : null
    // Mandatory: false
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    try {
      if (t.getCode() != AMQPTypeDecoder.NULL)
        additionalData = (AMQPBinary)t;
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'additionalData' in 'SaslOutcome' frame: "+e);
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
    addToList(l, mycode);
    addToList(l, additionalData);
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
   * Returns a value representation of this SaslOutcomeFrame.
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
    StringBuffer b = new StringBuffer("[SaslOutcome ");
    b.append(super.getValueString());
    b.append("]");
    return b.toString();
  }

  private String getDisplayString()
  {
    boolean _first = true;
    StringBuffer b = new StringBuffer();
    if (mycode != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("mycode=");
      b.append(mycode.getValueString());
    }
    if (additionalData != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("additionalData=");
      b.append(additionalData.getValueString());
    }
    return b.toString();
  }

  public String toString()
  {
    return "[SaslOutcome "+getDisplayString()+"]";
  }
}
