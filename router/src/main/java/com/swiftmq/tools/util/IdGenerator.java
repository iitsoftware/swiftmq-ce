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

package com.swiftmq.tools.util;

import java.util.Random;

public class IdGenerator
{
  long randomId = 0;
  long id = 0;

  private IdGenerator()
  {
    Random random = new Random();
    randomId = random.nextLong();
  }

  private static class InstanceHolder
  {
    public static IdGenerator instance = new IdGenerator();
  }

  public static IdGenerator getInstance()
  {
    return InstanceHolder.instance;
  }

  public synchronized String nextId(char delimiter)
  {
    StringBuffer b = new StringBuffer();
    b.append(randomId);
    b.append(delimiter);
    b.append(id++);
    return b.toString();
  }
}
