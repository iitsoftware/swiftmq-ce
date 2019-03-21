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

package com.swiftmq.impl.queue.standard.accounting;

import com.swiftmq.impl.queue.standard.SwiftletContext;
import com.swiftmq.swiftlet.accounting.*;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class QueueManagerSourceFactory implements AccountingSourceFactory
{
  SwiftletContext ctx = null;
  Map parms = null;

  public QueueManagerSourceFactory(SwiftletContext ctx)
  {
    this.ctx = ctx;
    parms = new HashMap();
    Parameter p = new Parameter("Flush Interval", "Flush Interval of Accounting Data in milliseconds", "60000", false, new ParameterVerifier()
    {
      public void verify(Parameter parameter, String value) throws InvalidValueException
      {
        try
        {
          long ms = Long.parseLong(value);
        } catch (NumberFormatException e)
        {
          throw new InvalidValueException(e.toString());
        }
      }
    });
    parms.put(p.getName(), p);
    p = new Parameter("Queue Name Filter", "Regular Expression to filter Queue Names", null, false, new ParameterVerifier()
    {
      public void verify(Parameter parameter, String value) throws InvalidValueException
      {
        try
        {
          Pattern.compile(value);
        } catch (Exception e)
        {
          throw new InvalidValueException(e.toString());
        }
      }
    });
    parms.put(p.getName(), p);
  }

  public boolean isSingleton()
  {
    return true;
  }

  public String getGroup()
  {
    return "Queue Manager";
  }

  public String getName()
  {
    return "QueueManagerSourceFactory";
  }

  public Map getParameters()
  {
    return parms;
  }

  public AccountingSource create(Map map) throws Exception
  {
    long flushInterval = Long.parseLong((String) map.get("Flush Interval"));
    String queueNameFilter = (String) map.get("Queue Name Filter");
    boolean queueNameFilterNegate = queueNameFilter != null && queueNameFilter.startsWith("!");
    if (queueNameFilterNegate)
      queueNameFilter = queueNameFilter.substring(1);
    return new QueueManagerSource(ctx, flushInterval,
        new AccountingProfile(ctx, queueNameFilter == null ? null : Pattern.compile(queueNameFilter), queueNameFilterNegate));

  }
}
