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

/**
 *  The LifetimePolicy interface.
 *
 *  @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 *  @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 **/

public interface LifetimePolicyIF
{
  /**
   * Accept method for a LifetimePolicy visitor.
   *
   * @param visitor LifetimePolicy visitor
   */
  public void accept(LifetimePolicyVisitor visitor);

  /**
   * Returns the predicted size of this LifetimePolicyIF. The predicted size may be greater than the actual size
   * but it can never be less.
   *
   * @return predicted size
   */
  public int getPredictedSize();

  /**
   * Returns a value representation of this LifetimePolicyIF.
   *
   * @return value representation
   */
  public String getValueString();
}
