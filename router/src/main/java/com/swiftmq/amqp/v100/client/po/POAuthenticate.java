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

package com.swiftmq.amqp.v100.client.po;

import com.swiftmq.amqp.v100.client.ConnectionVisitor;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.POVisitor;

public class POAuthenticate extends POObject
{
  String mechanism = null;
  String username = null;
  String password = null;

  public POAuthenticate(Semaphore semaphore, String mechanism, String username, String password)
  {
    super(null, semaphore);
    this.mechanism = mechanism;
    this.username = username;
    this.password = password;
  }

  public String getMechanism()
  {
    return mechanism;
  }

  public String getUsername()
  {
    return username;
  }

  public String getPassword()
  {
    return password;
  }

  public void accept(POVisitor visitor)
  {
    ((ConnectionVisitor) visitor).visit(this);
  }

  public String toString()
  {
    return "[POAuthenticate, mechanism="+mechanism+", username=" + username + ", password=" + password + "]";
  }
}
