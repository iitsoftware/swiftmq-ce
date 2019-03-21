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

package com.swiftmq.jms.primitives;

import com.swiftmq.tools.dump.Dumpable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class _Long implements Dumpable, Primitive
{
  Long value = null;

  public _Long()
  {
  }

  public _Long(long value)
  {
    this.value = new Long(value);
  }

  public _Long(String s)
  {
    this.value = Long.valueOf(s);
  }

  public long longValue()
  {
    return value.longValue();
  }

  public Object getObject()
  {
    return value;
  }

  public int getDumpId()
  {
    return LONG;
  }

  public void writeContent(DataOutput out)
      throws IOException
  {
    out.writeLong(value.longValue());
  }

  public void readContent(DataInput in)
      throws IOException
  {
    value = new Long(in.readLong());
  }

  public String toString()
  {
    return value.toString();
  }
}
