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

package com.swiftmq.impl.store.standard.index;

public abstract class IndexEntry
{
  Comparable key = null;
  int rootPageNo = -1;
  boolean valid = true;

  public void setKey(Comparable c)
  {
    key = c;
  }

  public Comparable getKey()
  {
    return key;
  }

  public void setRootPageNo(int rootPageNo)
  {
    this.rootPageNo = rootPageNo;
  }

  public int getRootPageNo()
  {
    return rootPageNo;
  }

  public void setValid(boolean valid)
  {
    this.valid = valid;
  }

  public boolean isValid()
  {
    return valid;
  }

  public abstract int getLength();

  public abstract void writeContent(byte[] b, int offset);

  public abstract void readContent(byte[] b, int offset);

  public boolean equals(Object that)
  {
    return ((IndexEntry) that).key.equals(key);
  }

  public String toString()
  {
    return "key=" + key + ", rootPageNo=" + rootPageNo + ", valid=" + valid;
  }
}

