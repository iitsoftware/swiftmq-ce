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
import com.swiftmq.jms.MapMessageImpl;
import com.swiftmq.swiftlet.accounting.AccountingSink;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;

public class FileSink implements AccountingSink
{
  private static final SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMddHHmmssSSS");
  SwiftletContext ctx = null;
  String directoryName = null;
  String fieldSeparator = null;
  String[] fieldOrder = null;
  int rollOverSize = 0;
  PrintWriter writer = null;
  int length = 0;

  public FileSink(SwiftletContext ctx, String directoryName, String fieldSeparator, String[] fieldOrder, int rollOverSize)
  {
    this.ctx = ctx;
    this.directoryName = directoryName;
    this.fieldSeparator = fieldSeparator;
    this.fieldOrder = fieldOrder;
    this.rollOverSize = rollOverSize;
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/created");
  }

  private void newFile() throws Exception
  {
    if (writer != null)
      writer.close();
    String filename = directoryName + File.separatorChar + "filesink-" + fmt.format(new Date()) + ".txt";
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/newFile, filename=" + filename);
    writer = new PrintWriter(new FileWriter(filename, true), true);
    length = 0;
  }

  private void writeToFile(MapMessageImpl msg) throws Exception
  {
    if (length / 1024 >= rollOverSize)
      newFile();
    StringBuffer b = new StringBuffer();
    boolean first = true;
    if (fieldOrder == null)
    {
      for (Enumeration iter = msg.getMapNames(); iter.hasMoreElements();)
      {
        String name = (String) iter.nextElement();
        Object value = msg.getObject(name);
        if (!first)
          b.append(fieldSeparator);
        b.append(value);
        first = false;
      }
    } else
    {
      for (int i = 0; i < fieldOrder.length; i++)
      {
        String name = fieldOrder[i];
        Object value = msg.getObject(name);
        if (value == null)
          value = "['" + name + "' not found" + "]";
        if (!first)
          b.append(fieldSeparator);
        b.append(value);
        first = false;
      }
    }
    length += b.length();
    writer.println(b.toString());
  }

  public void add(MapMessageImpl msg) throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/add, msg=" + msg);
    if (writer == null)
      newFile();
    writeToFile(msg);
  }

  public void close()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/close");
    if (writer != null)
      writer.close();
    writer = null;
  }

  public String toString()
  {
    return "FileSink, directoryName=" + directoryName;
  }
}
