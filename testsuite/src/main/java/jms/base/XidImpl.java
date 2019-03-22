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

package jms.base;

import javax.transaction.xa.Xid;

public class XidImpl
    implements Xid
{
  String branchQ = null;
  static int fmtId = 0;
  int localId = 0;
  String globalTxId = null;

  public XidImpl()
  {
    branchQ = String.valueOf(getId());
    globalTxId = new Long(System.currentTimeMillis()).toString();
    localId = 0;
  }

  public XidImpl(String branch)
  {
    branchQ = branch + "/" + getId();
    globalTxId = new Long(System.currentTimeMillis()).toString();
    localId = 0;
  }

  private static synchronized int getId()
  {
    fmtId++;
    return fmtId;
  }

  public byte[] getBranchQualifier()
  {
    return branchQ.getBytes();
  }

  public int getFormatId()
  {
    return localId;
  }

  public byte[] getGlobalTransactionId()
  {
    return globalTxId.getBytes();
  }

  public String toString()
  {
    return branchQ + "/" + fmtId + "/" + globalTxId;
  }
}
