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

package com.swiftmq.amqp.v100.generated.transport.definitions;

import com.swiftmq.amqp.v100.types.*;
import com.swiftmq.amqp.v100.transport.*;
import com.swiftmq.amqp.v100.generated.*;
import com.swiftmq.amqp.v100.generated.transport.definitions.Error;
import com.swiftmq.amqp.v100.generated.transport.performatives.*;
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
 *
 *  @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 **/

public class AmqpError extends AMQPSymbol
       implements ErrorConditionIF
{

  public static final Set POSSIBLE_VALUES = new HashSet();
  static {
    POSSIBLE_VALUES.add("amqp:internal-error");
    POSSIBLE_VALUES.add("amqp:not-found");
    POSSIBLE_VALUES.add("amqp:unauthorized-access");
    POSSIBLE_VALUES.add("amqp:decode-error");
    POSSIBLE_VALUES.add("amqp:resource-limit-exceeded");
    POSSIBLE_VALUES.add("amqp:not-allowed");
    POSSIBLE_VALUES.add("amqp:invalid-field");
    POSSIBLE_VALUES.add("amqp:not-implemented");
    POSSIBLE_VALUES.add("amqp:resource-locked");
    POSSIBLE_VALUES.add("amqp:precondition-failed");
    POSSIBLE_VALUES.add("amqp:resource-deleted");
    POSSIBLE_VALUES.add("amqp:illegal-state");
    POSSIBLE_VALUES.add("amqp:frame-size-too-small");
  }

  public static final AmqpError INTERNAL_ERROR = new AmqpError("amqp:internal-error");
  public static final AmqpError NOT_FOUND = new AmqpError("amqp:not-found");
  public static final AmqpError UNAUTHORIZED_ACCESS = new AmqpError("amqp:unauthorized-access");
  public static final AmqpError DECODE_ERROR = new AmqpError("amqp:decode-error");
  public static final AmqpError RESOURCE_LIMIT_EXCEEDED = new AmqpError("amqp:resource-limit-exceeded");
  public static final AmqpError NOT_ALLOWED = new AmqpError("amqp:not-allowed");
  public static final AmqpError INVALID_FIELD = new AmqpError("amqp:invalid-field");
  public static final AmqpError NOT_IMPLEMENTED = new AmqpError("amqp:not-implemented");
  public static final AmqpError RESOURCE_LOCKED = new AmqpError("amqp:resource-locked");
  public static final AmqpError PRECONDITION_FAILED = new AmqpError("amqp:precondition-failed");
  public static final AmqpError RESOURCE_DELETED = new AmqpError("amqp:resource-deleted");
  public static final AmqpError ILLEGAL_STATE = new AmqpError("amqp:illegal-state");
  public static final AmqpError FRAME_SIZE_TOO_SMALL = new AmqpError("amqp:frame-size-too-small");

  /**
   * Constructs a AmqpError.
   *
   * @param initValue initial value
   */
  public AmqpError(String initValue) 
  {
    super(initValue);
  }

  /**
   * Accept method for a ErrorCondition visitor.
   *
   * @param visitor ErrorCondition visitor
   */
   public void accept(ErrorConditionVisitor visitor)
   {
     visitor.visit(this);
   }


  public String toString()
  {
    return "[AmqpError " + super.toString() + "]";
  }
}
