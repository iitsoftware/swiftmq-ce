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

package com.swiftmq.amqp.v100.generated.messaging.delivery_state;

import com.swiftmq.amqp.v100.types.*;
import com.swiftmq.amqp.v100.transport.*;
import com.swiftmq.amqp.v100.generated.*;
import com.swiftmq.amqp.v100.generated.transport.definitions.Error;
import com.swiftmq.amqp.v100.generated.transport.performatives.*;
import com.swiftmq.amqp.v100.generated.transport.definitions.*;
import com.swiftmq.amqp.v100.generated.messaging.message_format.*;
import com.swiftmq.amqp.v100.generated.messaging.addressing.*;
import com.swiftmq.amqp.v100.generated.security.sasl.*;
import com.swiftmq.amqp.v100.generated.transactions.coordination.*;
import com.swiftmq.amqp.v100.generated.provides.global_tx_id_types.*;
import com.swiftmq.amqp.v100.generated.filter.filter_types.*;
import java.io.*;
import java.util.*;

/**
 *  The DeliveryState visitor.
 *
 *  @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 **/

public interface DeliveryStateVisitor
{

  /**
   * Visitor method for a Received type object.
   *
   * @param impl a Received type object
   */
  public void visit(Received impl);

  /**
   * Visitor method for a Accepted type object.
   *
   * @param impl a Accepted type object
   */
  public void visit(Accepted impl);

  /**
   * Visitor method for a Rejected type object.
   *
   * @param impl a Rejected type object
   */
  public void visit(Rejected impl);

  /**
   * Visitor method for a Released type object.
   *
   * @param impl a Released type object
   */
  public void visit(Released impl);

  /**
   * Visitor method for a Modified type object.
   *
   * @param impl a Modified type object
   */
  public void visit(Modified impl);

  /**
   * Visitor method for a Declared type object.
   *
   * @param impl a Declared type object
   */
  public void visit(Declared impl);

  /**
   * Visitor method for a TransactionalState type object.
   *
   * @param impl a TransactionalState type object
   */
  public void visit(TransactionalState impl);
}
