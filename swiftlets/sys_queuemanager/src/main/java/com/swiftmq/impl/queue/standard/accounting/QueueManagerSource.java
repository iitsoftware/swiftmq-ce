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
import com.swiftmq.jms.MapMessageImpl;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.accounting.AccountingSink;
import com.swiftmq.swiftlet.accounting.AccountingSource;
import com.swiftmq.swiftlet.accounting.StopListener;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.util.IdGenerator;

import java.net.InetAddress;
import java.util.Date;

public class QueueManagerSource implements AccountingSource, TimerListener
{
  private static final String PROP_SWIFTLET = "swiftlet";
  private static final String PROP_TIMESTAMP = "timestamp";
  private static final String PROP_ROUTERNAME = "routername";
  private static final String PROP_ROUTERHOSTNAME = "routerhostname";

  SwiftletContext ctx = null;
  StopListener stopListener = null;
  AccountingSink accountingSink = null;
  long flushInterval = 0;
  com.swiftmq.impl.queue.standard.accounting.AccountingProfile accountingProfile = null;
  String uniqueueId = IdGenerator.getInstance().nextId('-');
  long count = 0;
  String routerName = null;
  String routerHostName = null;

  public QueueManagerSource(SwiftletContext ctx, long flushInterval, AccountingProfile accountingProfile)
  {
    this.ctx = ctx;
    this.flushInterval = flushInterval;
    this.accountingProfile = accountingProfile;
    routerName = SwiftletManager.getInstance().getRouterName();
    try
    {
      routerHostName = InetAddress.getLocalHost().getHostName();
    } catch (Exception e)
    {
      routerHostName = "unknown";
    }
    accountingProfile.setSource(this);
  }

  public void performTimeAction()
  {
    ctx.queueManager.flushAccounting();
  }

  public void setStopListener(StopListener stopListener)
  {
    this.stopListener = stopListener;
  }

  public synchronized void startAccounting(AccountingSink accountingSink) throws Exception
  {
    this.accountingSink = accountingSink;
    ctx.queueManager.setAccountingProfile(accountingProfile);
    ctx.timerSwiftlet.addTimerListener(flushInterval, this);
  }

  public synchronized void stopAccounting() throws Exception
  {
    accountingSink = null;
    ctx.queueManager.setAccountingProfile(null);
    ctx.timerSwiftlet.removeTimerListener(this);
  }

  public synchronized void send(QueueCollector collector)
  {
    if (accountingSink != null)
    {
      try
      {
        MapMessageImpl msg = new MapMessageImpl();
        msg.setJMSMessageID(uniqueueId + (count++));
        if (count == Long.MAX_VALUE)
          count = 0;
        msg.setJMSTimestamp(System.currentTimeMillis());
        msg.setString(PROP_SWIFTLET, ctx.queueManager.getName());
        msg.setStringProperty(PROP_SWIFTLET, ctx.queueManager.getName());
        msg.setString(PROP_TIMESTAMP, QueueCollector.fmt.format(new Date()));
        msg.setString(PROP_ROUTERNAME, routerName);
        msg.setStringProperty(PROP_ROUTERNAME, routerName);
        msg.setString(PROP_ROUTERHOSTNAME, routerHostName);
        msg.setStringProperty(PROP_ROUTERHOSTNAME, routerHostName);
        collector.dumpToMapMessage(msg);
        accountingSink.add(msg);
        collector.clear();
      } catch (Exception e)
      {
        if (stopListener != null)
          stopListener.sourceStopped(this, e);
      }
    }
  }

}
