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

package com.swiftmq.impl.routing.single.accounting;

import com.swiftmq.impl.routing.single.SwiftletContext;
import com.swiftmq.jms.MapMessageImpl;
import com.swiftmq.swiftlet.accounting.AccountingSink;
import com.swiftmq.swiftlet.accounting.AccountingSource;
import com.swiftmq.swiftlet.accounting.StopListener;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.util.IdGenerator;

import java.net.InetAddress;

public class RoutingSource implements AccountingSource, TimerListener
{
  private static final String PROP_SWIFTLET = "swiftlet";
  private static final String PROP_TIMESTAMP = "timestamp";
  private static final String PROP_LOCALROUTERNAME = "localroutername";
  private static final String PROP_LOCALHOSTNAME = "localhostname";
  private static final String PROP_CONNECTEDROUTERNAME = "connectedroutername";
  private static final String PROP_CONNECTEDHOSTNAME = "connectedhostname";
  private static final String PROP_SMQPRVERSION = "smqprversion";

  SwiftletContext ctx = null;
  StopListener stopListener = null;
  AccountingSink accountingSink = null;
  long flushInterval = 0;
  AccountingProfile accountingProfile = null;
  String uniqueueId = IdGenerator.getInstance().nextId('-');
  long count = 0;
  String routerHostName = null;

  public RoutingSource(SwiftletContext ctx, long flushInterval, AccountingProfile accountingProfile)
  {
    this.ctx = ctx;
    this.flushInterval = flushInterval;
    this.accountingProfile = accountingProfile;
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
    ctx.routingSwiftlet.flushAccounting();
  }

  public void setStopListener(StopListener stopListener)
  {
    this.stopListener = stopListener;
  }

  public synchronized void startAccounting(AccountingSink accountingSink) throws Exception
  {
    this.accountingSink = accountingSink;
    ctx.routingSwiftlet.setAccountingProfile(accountingProfile);
    ctx.timerSwiftlet.addTimerListener(flushInterval, this);
  }

  public synchronized void stopAccounting() throws Exception
  {
    accountingSink = null;
    ctx.routingSwiftlet.setAccountingProfile(null);
    ctx.timerSwiftlet.removeTimerListener(this);
  }

  public synchronized void send(String timestamp, String localRouter, String connectedRouter, String connectedHostName, String smqprVersion, DestinationCollector collector)
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
        msg.setString(PROP_SWIFTLET, ctx.routingSwiftlet.getName());
        msg.setStringProperty(PROP_SWIFTLET, ctx.routingSwiftlet.getName());
        msg.setString(PROP_TIMESTAMP, timestamp);
        msg.setString(PROP_LOCALROUTERNAME, localRouter);
        msg.setStringProperty(PROP_LOCALROUTERNAME, localRouter);
        msg.setString(PROP_LOCALHOSTNAME, routerHostName);
        msg.setStringProperty(PROP_LOCALHOSTNAME, routerHostName);
        msg.setString(PROP_CONNECTEDROUTERNAME, connectedRouter);
        msg.setStringProperty(PROP_CONNECTEDROUTERNAME, connectedRouter);
        msg.setString(PROP_CONNECTEDHOSTNAME, connectedHostName);
        msg.setStringProperty(PROP_CONNECTEDHOSTNAME, connectedHostName);
        msg.setString(PROP_SMQPRVERSION, smqprVersion);
        msg.setStringProperty(PROP_SMQPRVERSION, smqprVersion);
        collector.dumpToMapMessage(msg);
        accountingSink.add(msg);
      } catch (Exception e)
      {
        if (stopListener != null)
          stopListener.sourceStopped(this, e);
      }
    }
  }

}
