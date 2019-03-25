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

package com.swiftmq.impl.amqp.accounting;

import com.swiftmq.impl.amqp.SwiftletContext;
import com.swiftmq.jms.MapMessageImpl;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.accounting.AccountingSink;
import com.swiftmq.swiftlet.accounting.AccountingSource;
import com.swiftmq.swiftlet.accounting.StopListener;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.util.IdGenerator;

import java.net.InetAddress;

public class AMQPSource implements AccountingSource, TimerListener {
    private static final String PROP_SWIFTLET = "swiftlet";
    private static final String PROP_AMQPVERSION = "amqpversion";
    private static final String PROP_TIMESTAMP = "timestamp";
    private static final String PROP_ROUTERNAME = "routername";
    private static final String PROP_ROUTERHOSTNAME = "routerhostname";
    private static final String PROP_USERNAME = "username";
    private static final String PROP_CLIENTID = "clientid";
    private static final String PROP_CLIENTHOSTNAME = "clienthostname";

    SwiftletContext ctx = null;
    StopListener stopListener = null;
    AccountingSink accountingSink = null;
    long flushInterval = 0;
    AccountingProfile accountingProfile = null;
    String uniqueueId = IdGenerator.getInstance().nextId('-');
    long count = 0;
    String routerHostName = null;

    public AMQPSource(SwiftletContext ctx, long flushInterval, AccountingProfile accountingProfile) {
        this.ctx = ctx;
        this.flushInterval = flushInterval;
        this.accountingProfile = accountingProfile;
        try {
            routerHostName = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            routerHostName = "unknown";
        }
        accountingProfile.setSource(this);
    }

    public void performTimeAction() {
        ctx.amqpSwiftlet.flushAccounting();
    }

    public synchronized void setStopListener(StopListener stopListener) {
        this.stopListener = stopListener;
    }

    public synchronized void startAccounting(AccountingSink accountingSink) throws Exception {
        this.accountingSink = accountingSink;
        ctx.amqpSwiftlet.setAccountingProfile(accountingProfile);
        ctx.timerSwiftlet.addTimerListener(flushInterval, this);
    }

    public synchronized void stopAccounting() throws Exception {
        accountingSink = null;
        ctx.timerSwiftlet.removeTimerListener(this);
        ctx.amqpSwiftlet.setAccountingProfile(null);
    }

    public synchronized void send(String timestamp, String userName, String clientId, String remoteHostName, String amqpVersion, DestinationCollector collector) {
        if (accountingSink != null) {
            try {
                MapMessageImpl msg = new MapMessageImpl();
                msg.setJMSMessageID(uniqueueId + (count++));
                if (count == Long.MAX_VALUE)
                    count = 0;
                msg.setJMSTimestamp(System.currentTimeMillis());
                msg.setString(PROP_SWIFTLET, ctx.amqpSwiftlet.getName());
                msg.setStringProperty(PROP_SWIFTLET, ctx.amqpSwiftlet.getName());
                msg.setString(PROP_AMQPVERSION, amqpVersion);
                msg.setStringProperty(PROP_AMQPVERSION, amqpVersion);
                msg.setString(PROP_TIMESTAMP, timestamp);
                msg.setString(PROP_ROUTERNAME, SwiftletManager.getInstance().getRouterName());
                msg.setStringProperty(PROP_ROUTERNAME, SwiftletManager.getInstance().getRouterName());
                msg.setString(PROP_ROUTERHOSTNAME, routerHostName);
                msg.setStringProperty(PROP_ROUTERHOSTNAME, routerHostName);
                if (userName != null)
                    msg.setString(PROP_USERNAME, userName);
                else
                    msg.setString(PROP_USERNAME, "anonymous");
                msg.setStringProperty(PROP_USERNAME, msg.getString(PROP_USERNAME));
                if (clientId != null) {
                    msg.setString(PROP_CLIENTID, clientId);
                    msg.setStringProperty(PROP_CLIENTID, clientId);
                } else {
                    msg.setString(PROP_CLIENTID, "not set");
                    msg.setStringProperty(PROP_CLIENTID, "not set");
                }
                if (remoteHostName != null) {
                    msg.setString(PROP_CLIENTHOSTNAME, remoteHostName);
                    msg.setStringProperty(PROP_CLIENTHOSTNAME, remoteHostName);
                } else {
                    msg.setString(PROP_CLIENTHOSTNAME, "not set");
                    msg.setStringProperty(PROP_CLIENTHOSTNAME, "not set");
                }
                collector.dumpToMapMessage(msg);
                accountingSink.add(msg);
            } catch (Exception e) {
                if (stopListener != null)
                    stopListener.sourceStopped(this, e);
            }
        }
    }
}
