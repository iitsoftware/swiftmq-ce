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
 * Send the SASL challenge data as defined by the SASL specification.
 * </p><p>
 * </p>
 *
 *  @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 **/

public class SaslChallengeFrame extends AMQPFrame
       implements SaslFrameIF
{
  public static String DESCRIPTOR_NAME = "amqp:sasl-challenge:list";
  public static long DESCRIPTOR_CODE = 0x00000000L<<32 | 0x00000042L;

  public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
  public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);
  
  AMQPList body = null;
  boolean dirty = false;

  AMQPBinary challenge =  null;

  /**
   * Constructs a SaslChallengeFrame.
   *
   * @param channel the channel id
   * @param body the frame body
   */
  public SaslChallengeFrame(int channel, AMQPList body) throws Exception
  {
    super(channel);
    setTypeCode(TYPE_CODE_SASL_FRAME);
    this.body = body;
    if (body != null)
      decode();
  }

  /**
   * Constructs a SaslChallengeFrame.
   *
   * @param channel the channel id
   */
  public SaslChallengeFrame(int channel)
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
   * Sets the mandatory Challenge field.
   *
   * <p>
   * </p><p>
   * </p><p>
   * Challenge information, a block of opaque binary data passed to the security
   * mechanism.
   * </p><p>
   * </p><p>
   * </p><p>
   * </p>
   * @param challenge Challenge
   */
  public void setChallenge(AMQPBinary challenge)
  {
    dirty = true;
    this.challenge = challenge;
  }

  /**
   * Returns the mandatory Challenge field.
   *
   * @return Challenge
   */
  public AMQPBinary getChallenge()
  {
    return challenge;
  }


  /**
   * Returns the predicted size of this SaslChallengeFrame. The predicted size may be greater than the actual size
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
      n += (challenge != null?challenge.getPredictedSize():1);
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


    // Field: challenge
    // Type     : binary, converted: AMQPBinary
    // Basetype : binary
    // Default  : null
    // Mandatory: true
    // Multiple : false
    // Factory  : ./.
    if (idx >= l.size())
      return;
    t = (AMQPType) l.get(idx++);
    if (t.getCode() == AMQPTypeDecoder.NULL)
      throw new Exception("Mandatory field 'challenge' in 'SaslChallenge' frame is NULL");
    try {
      challenge = (AMQPBinary)t;
    } catch (ClassCastException e)
    {
      throw new Exception("Invalid type of field 'challenge' in 'SaslChallenge' frame: "+e);
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
    addToList(l, challenge);
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
   * Returns a value representation of this SaslChallengeFrame.
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
    StringBuffer b = new StringBuffer("[SaslChallenge ");
    b.append(super.getValueString());
    b.append("]");
    return b.toString();
  }

  private String getDisplayString()
  {
    boolean _first = true;
    StringBuffer b = new StringBuffer();
    if (challenge != null)
    {
      if (!_first)
        b.append(", ");
      else
        _first = false;
      b.append("challenge=");
      b.append(challenge.getValueString());
    }
    return b.toString();
  }

  public String toString()
  {
    return "[SaslChallenge "+getDisplayString()+"]";
  }
}
