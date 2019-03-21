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
import java.util.Map;

public class ExchangeRegistry
{
  SwiftletContext ctx = null;
  Map exchanges = new HashMap<String, Exchange>();
  Exchange defaultDirect = null;

  public ExchangeRegistry(SwiftletContext ctx)
  {
    this.ctx = ctx;
    defaultDirect = new Exchange()
    {
      public int getType()
      {
        return DIRECT;
      }
    };
    exchanges.put("", defaultDirect);
    exchanges.put("amq.direct", defaultDirect);
  }

  public synchronized Exchange get(String name)
  {
    return (Exchange) exchanges.get(name);
  }

  public synchronized Exchange declare(String name, ExchangeFactory exchangeFactory) throws Exception
  {
    Exchange exchange = (Exchange) exchanges.get(name);
    if (exchange == null && exchangeFactory != null)
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/create exchange: " + name);
      exchange = exchangeFactory.create();
      exchanges.put(name, exchange);
    }
    return exchange;
  }

  public synchronized void delete(String name, boolean ifUnused) throws Exception
  {
    Exchange exchange = (Exchange) exchanges.get(name);
    if (exchange != null && exchange != defaultDirect)
    {
      exchanges.remove(name);
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/delete exchange: " + name);
    }
  }

  public String toString()
  {
    final StringBuffer sb = new StringBuffer();
    sb.append("ExchangeRegistry");
    return sb.toString();
  }
}
