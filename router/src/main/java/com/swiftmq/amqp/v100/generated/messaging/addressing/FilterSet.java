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
 * A set of named filters. Every key in the map must be of type  , every value must be either   or of a
 * described type which provides the archetype  filter . A filter acts as a function on
 * a message which returns a boolean result indicating whether the message may pass through
 * that filter or not.  A message will pass through a filter-set if and only if it passes
 * through each of the named filters. If the value for a given key is null, this acts as if
 * there were no such key present (i.e., all messages pass through the null filter).
 * </p><p>
 * </p><p>
 * Filter types are a defined extension point. The filter types that a given
 * supports will be indicated by the capabilities of the
 * .
 * A registry of commonly defined filter types and their capabilities is maintained
 * [ AMQPFILTERS ].
 * </p><p>
 * </p><p>
 * </p>
 *
 *  @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 **/

public class FilterSet extends AMQPMap
{


  /**
   * Constructs a FilterSet.
   *
   * @param initValue initial value
   * @exception error during initialization
   */
  public FilterSet(Map initValue) throws IOException
  {
    super(initValue);
  }


  public String toString()
  {
    return "[FilterSet " + super.toString() + "]";
  }
}
