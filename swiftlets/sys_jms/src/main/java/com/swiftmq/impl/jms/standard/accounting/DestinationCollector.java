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

package com.swiftmq.impl.jms.standard.accounting;

import com.swiftmq.jms.MapMessageImpl;
import com.swiftmq.swiftlet.SwiftletManager;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DestinationCollector
{
  public static final SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMddHHmmssSSS");
  public static final String ATYPE_PRODUCER = "PRODUCER";
  public static final String ATYPE_CONSUMER = "CONSUMER";
  public static final String DTYPE_QUEUE = "QUEUE";
  public static final String DTYPE_TOPIC = "TOPIC";

  private static final String PROP_DESTNAME = "destinationname";
  private static final String PROP_DESTTYP = "destinationtype";
  private static final String PROP_ACCTYP = "accountingtype";
  private static final String PROP_MESSAGES = "numbermessages";
  private static final String PROP_SIZE = "size";
  private static final String PROP_SIZE_KB = "sizekb";
  private static final String PROP_START_ACCOUNTING = "startaccounting";
  private static final String PROP_END_ACCOUNTING = "endaccounting";

  String key = null;
  String destinationName = null;
  String destinationType = null;
  String accountingType = null;
  long totalNumberMsgs = 0;
  long totalSize = 0;
  long txNumberMsgs = 0;
  long txSize = 0;
  long startAccountingTime = -1;
  long endAccountingTime = -1;

  public DestinationCollector(String key, String destinationName, String destinationType, String accountingType)
  {
    this.key = key;
    this.destinationName = destinationName;
    this.destinationType = destinationType;
    this.accountingType = accountingType;
    if (destinationType.equals(DTYPE_QUEUE) && destinationName.indexOf('@') == -1)
      this.destinationName = this.destinationName + '@' + SwiftletManager.getInstance().getRouterName();
  }

  void dumpToMapMessage(MapMessageImpl msg) throws Exception
  {
    msg.setStringProperty(PROP_START_ACCOUNTING, fmt.format(new Date(startAccountingTime)));
    msg.setStringProperty(PROP_END_ACCOUNTING, fmt.format(new Date(endAccountingTime)));
    msg.setStringProperty(PROP_DESTNAME, destinationName);
    msg.setStringProperty(PROP_DESTTYP, destinationType);
    msg.setStringProperty(PROP_ACCTYP, accountingType);
    msg.setString(PROP_START_ACCOUNTING, fmt.format(new Date(startAccountingTime)));
    msg.setString(PROP_END_ACCOUNTING, fmt.format(new Date(endAccountingTime)));
    msg.setString(PROP_DESTNAME, destinationName);
    msg.setString(PROP_DESTTYP, destinationType);
    msg.setString(PROP_ACCTYP, accountingType);
    msg.setString(PROP_MESSAGES, new Long(totalNumberMsgs).toString());
    msg.setString(PROP_SIZE, new Long(totalSize).toString());
    msg.setString(PROP_SIZE_KB, new Double((double) totalSize / 1024.0).toString());
  }

  public String getKey()
  {
    return key;
  }

  public String getDestinationName()
  {
    return destinationName;
  }

  public String getDestinationType()
  {
    return destinationType;
  }

  public String getAccountingType()
  {
    return accountingType;
  }

  public synchronized long getTotalNumberMsgs()
  {
    return totalNumberMsgs;
  }

  public synchronized long getTotalSize()
  {
    return totalSize;
  }

  public synchronized long getStartAccountingTime()
  {
    return startAccountingTime;
  }

  public synchronized long getEndAccountingTime()
  {
    return endAccountingTime;
  }

  public synchronized boolean isDirty()
  {
    return totalNumberMsgs > 0;
  }

  public synchronized void incTotal(long nMsgs, long size)
  {
    if (size <= 0)
      return;
    long time = System.currentTimeMillis();
    if (startAccountingTime == -1)
      startAccountingTime = time;
    endAccountingTime = time;
    totalNumberMsgs += nMsgs;
    totalSize += size;
  }

  public synchronized void incTx(long nMsgs, long size)
  {
    if (size <= 0)
      return;
    txNumberMsgs += nMsgs;
    txSize += size;
  }

  public synchronized void commit()
  {
    long time = System.currentTimeMillis();
    if (startAccountingTime == -1)
      startAccountingTime = time;
    endAccountingTime = time;
    totalNumberMsgs += txNumberMsgs;
    totalSize += txSize;
    txNumberMsgs = 0;
    txSize = 0;
  }

  public synchronized void abort()
  {
    txNumberMsgs = 0;
    txSize = 0;
  }

  public synchronized void clear()
  {
    startAccountingTime = -1;
    endAccountingTime = -1;
    totalNumberMsgs = 0;
    totalSize = 0;
  }

  public String toString()
  {
    return "[DestinationCollector, key=" + key + ", totalNumberMsgs=" + totalNumberMsgs + ", totalSize=" + totalSize + "]";
  }
}
