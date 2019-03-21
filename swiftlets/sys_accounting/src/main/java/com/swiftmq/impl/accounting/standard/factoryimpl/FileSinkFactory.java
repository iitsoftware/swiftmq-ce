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

package com.swiftmq.impl.accounting.standard.factoryimpl;

import com.swiftmq.impl.accounting.standard.SwiftletContext;
import com.swiftmq.swiftlet.accounting.*;
import com.swiftmq.util.SwiftUtilities;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class FileSinkFactory implements AccountingSinkFactory
{
  SwiftletContext ctx = null;
  Map parms = null;

  public FileSinkFactory(SwiftletContext ctx)
  {
    this.ctx = ctx;
    parms = new HashMap();
    Parameter p = new Parameter("Directory Name", "Name of the directory where the files should be stored", null, true, new ParameterVerifier()
    {
      public void verify(Parameter parameter, String value) throws InvalidValueException
      {
        File d = new File(value);
        if (!d.exists())
          throw new InvalidValueException(parameter.getName() + ": Directory '" + value + "' not found!");
        if (!d.isDirectory())
          throw new InvalidValueException(parameter.getName() + ": File '" + value + "' is not a directory!");
      }
    });
    parms.put(p.getName(), p);
    p = new Parameter("Field Separator", "A separator character or string to separate the field values or TAB for tabulator char", "TAB", false, null);
    parms.put(p.getName(), p);
    p = new Parameter("Field Order", "A comma-seperated list of field names to select/order fields out of the message", null, false, null);
    parms.put(p.getName(), p);
    p = new Parameter("Rollover Size", "Maximum size in KB to rollover to a new file", "10240", false, new ParameterVerifier()
    {
      public void verify(Parameter parameter, String value) throws InvalidValueException
      {
        try
        {
          Integer.parseInt(value);
        } catch (NumberFormatException e)
        {
          throw new InvalidValueException(parameter.getName() + ": Unable to parse an int out of this crap: " + value);
        }
      }
    });
    parms.put(p.getName(), p);
  }

  public boolean isSingleton()
  {
    return false;
  }

  public String getGroup()
  {
    return "Accounting";
  }

  public String getName()
  {
    return "FileSinkFactory";
  }

  public Map getParameters()
  {
    return parms;
  }

  public AccountingSink create(Map map) throws Exception
  {
    String directoryName = (String) map.get("Directory Name");
    String fieldSeparator = (String) map.get("Field Separator");
    if (fieldSeparator.toUpperCase().equals("TAB"))
      fieldSeparator = "\t";
    String s = (String) map.get("Field Order");
    String[] fieldOrder = null;
    if (s != null)
      fieldOrder = SwiftUtilities.tokenize(s, ", ");
    int rollOverSize = Integer.parseInt((String) map.get("Rollover Size"));
    return new FileSink(ctx, directoryName, fieldSeparator, fieldOrder, rollOverSize);
  }
}
