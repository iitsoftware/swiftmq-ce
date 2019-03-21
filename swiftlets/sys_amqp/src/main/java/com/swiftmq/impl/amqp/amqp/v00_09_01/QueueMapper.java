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

package com.swiftmq.impl.amqp.amqp.v00_09_01;

import com.swiftmq.impl.amqp.SwiftletContext;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class QueueMapper
{
  SwiftletContext ctx = null;
  Map<String, String> mapping = new HashMap<String, String>();

  public QueueMapper(SwiftletContext ctx)
  {
    this.ctx = ctx;
  }

  public synchronized void map(String name, String mapTo)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/map " + name + " to " + mapTo);
    mapping.put(name, mapTo);
  }

  public synchronized void unmap(String name)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/unmap " + name);
    mapping.remove(name);
  }

  public synchronized String get(String name)
  {
    return mapping.get(name);
  }

  public synchronized void unmapTempQueue(String mapTo)
  {
    for (Iterator iter = mapping.entrySet().iterator(); iter.hasNext(); )
    {
      String s = (String) ((Map.Entry) iter.next()).getValue();
      if (s.equals(mapTo))
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/unmapTempQueue " + mapTo);
        iter.remove();
        break;
      }
    }
  }

  public String toString()
  {
    final StringBuffer sb = new StringBuffer();
    sb.append("QueueMapper");
    return sb.toString();
  }
}
