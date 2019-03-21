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

import com.swiftmq.jms.MapMessageImpl;

import java.text.SimpleDateFormat;
import java.util.Date;

public class QueueCollector
{
  public static final SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMddHHmmssSSS");
  public static final String ATYPE_PRODUCE = "PRODUCE";
  public static final String ATYPE_CONSUME = "CONSUME";
  public static final String DMODE_PERSISTENT = "PERSISTENT";
  public static final String DMODE_NON_PERSISTENT = "NON_PERSISTENT";

  private static final String PROP_QUEUENAME = "queuename";
  private static final String PROP_DMODE = "deliverymode";
  private static final String PROP_ATYPE = "accesstype";
  private static final String PROP_MESSAGES = "numbermessages";
  private static final String PROP_SIZE = "size";
  private static final String PROP_SIZE_KB = "sizekb";
  private static final String PROP_START_ACCOUNTING = "startaccounting";
  private static final String PROP_END_ACCOUNTING = "endaccounting";

  String queueName = null;
  String deliveryMode = null;
  String accessType = null;
  volatile long totalNumberMsgs = 0;
  volatile long totalSize = 0;
  volatile long txNumberMsgs = 0;
  volatile long txSize = 0;
  volatile long startAccountingTime = -1;
  volatile long endAccountingTime = -1;

  public QueueCollector(String queueName, String deliveryMode, String accessType)
  {
    this.queueName = queueName;
    this.deliveryMode = deliveryMode;
    this.accessType = accessType;
  }

  public synchronized void dumpToMapMessage(MapMessageImpl msg) throws Exception
  {
    msg.setStringProperty(PROP_START_ACCOUNTING, fmt.format(new Date(startAccountingTime)));
    msg.setStringProperty(PROP_END_ACCOUNTING, fmt.format(new Date(endAccountingTime)));
    msg.setStringProperty(PROP_QUEUENAME, queueName);
    msg.setStringProperty(PROP_DMODE, deliveryMode);
    msg.setStringProperty(PROP_ATYPE, accessType);
    msg.setString(PROP_START_ACCOUNTING, fmt.format(new Date(startAccountingTime)));
    msg.setString(PROP_END_ACCOUNTING, fmt.format(new Date(endAccountingTime)));
    msg.setString(PROP_QUEUENAME, queueName);
    msg.setString(PROP_DMODE, deliveryMode);
    msg.setString(PROP_ATYPE, accessType);
    msg.setString(PROP_MESSAGES, new Long(totalNumberMsgs).toString());
    msg.setString(PROP_SIZE, new Long(totalSize).toString());
    msg.setString(PROP_SIZE_KB, new Double((double) totalSize / 1024.0).toString());
    clear();
  }

  public boolean isDirty()
  {
    return totalNumberMsgs > 0;
  }

  public void incTotal(long nMsgs, long size)
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

  public void incTx(long nMsgs, long size)
  {
    if (size <= 0)
      return;
    txNumberMsgs += nMsgs;
    txSize += size;
  }

  public void commit()
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

  public void abort()
  {
    txNumberMsgs = 0;
    txSize = 0;
  }

  public void clear()
  {
    startAccountingTime = -1;
    endAccountingTime = -1;
    totalNumberMsgs = 0;
    totalSize = 0;
  }

  public String toString()
  {
    return "[QueueCollector, queueName=" + queueName + ", deliveryMode=" + deliveryMode + ", totalNumberMsgs=" + totalNumberMsgs + ", totalSize=" + totalSize + "]";
  }
}
