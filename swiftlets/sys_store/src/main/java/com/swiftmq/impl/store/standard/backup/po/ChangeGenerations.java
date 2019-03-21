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

package com.swiftmq.impl.store.standard.backup.po;

import com.swiftmq.tools.pipeline.*;
import com.swiftmq.tools.concurrent.Semaphore;

public class ChangeGenerations extends POObject
{
  int newGenerations = 0;

  public ChangeGenerations(Semaphore semaphore, int newGenerations)
  {
    super(null, semaphore);
    this.newGenerations = newGenerations;
  }

  public int getNewGenerations()
  {
    return newGenerations;
  }

  public void accept(POVisitor poVisitor)
  {
    ((EventVisitor)poVisitor).visit(this);
  }

  public String toString()
  {
    return "[ChangeGenerations, newGenerations="+newGenerations+"]";
  }
}
