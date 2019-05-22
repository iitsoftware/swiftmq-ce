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

package com.swiftmq.extension.jmsbridge;

import com.swiftmq.extension.jmsbridge.accounting.AccountingProfile;
import com.swiftmq.extension.jmsbridge.accounting.BridgeCollector;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityAddException;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.Property;

import javax.jms.Session;
import java.util.Date;

public class DestinationBridge implements ErrorListener {
    SwiftletContext ctx = null;
    ServerBridge server = null;
    Entity bridgeEntity;
    ConnectionCache connectionCache;
    String clientId;
    String tracePrefix;
    EntityList usageList = null;
    Entity usageEntity = null;
    Property messagesTransferedProp = null;
    Property lastTransferTimeProp = null;
    volatile int messagesTransfered = 0;
    volatile long lastTransferTime = -1;

    boolean localToRemote = false;
    BridgeSource source = null;
    BridgeSink sink = null;
    Session session = null;
    AccountingProfile accountingProfile = null;
    BridgeCollector bridgeCollector = null;

    volatile boolean destroyed = false;

    DestinationBridge(SwiftletContext ctx, ServerBridge server, Entity bridgeEntity, EntityList usageList, ConnectionCache connectionCache, String clientId, String tracePrefix)
            throws Exception {
        this.ctx = ctx;
        this.server = server;
        this.usageList = usageList;
        this.bridgeEntity = bridgeEntity;
        this.connectionCache = connectionCache;
        this.clientId = clientId;
        this.tracePrefix = tracePrefix;

        this.tracePrefix = tracePrefix + "/" + bridgeEntity.getName();

        String s = (String) bridgeEntity.getProperty("direction").getValue();
        localToRemote = s.equals("local_to_remote");
        AccountingProfile ap = ctx.bridgeSwiftlet.getAccountingProfile(server.getName(), bridgeEntity.getName());
        if (ap != null) {
            accountingProfile = ap;
            createCollector();
        }

        createBridge();
    }

    private void createBridge() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "createBridge ...");
        ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(BridgeSource.class.getClassLoader());
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "usageList = " + usageList);
        if (usageList != null) {
            usageEntity = usageList.createEntity();
            usageEntity.setName(bridgeEntity.getName());
            usageEntity.createCommands();
            try {
                usageList.addEntity(usageEntity);
            } catch (EntityAddException e) {
            }
            messagesTransferedProp = usageEntity.getProperty("number-messages-transfered");
            lastTransferTimeProp = usageEntity.getProperty("last-transfer-time");
        }
        String localName = (String) bridgeEntity.getProperty("localname").getValue();
        String localType = (String) bridgeEntity.getProperty("localtype").getValue();
        String remoteFactoryName = (String) bridgeEntity.getProperty("remotefactoryname").getValue();
        String remoteName = (String) bridgeEntity.getProperty("remotename").getValue();
        String remoteType = (String) bridgeEntity.getProperty("remotetype").getValue();
        String persistenceMode = (String) bridgeEntity.getProperty("transferpersistence").getValue();
        String durName = (String) bridgeEntity.getProperty("durablename").getValue();

        if (localToRemote) {
            if (localType.equals("queue"))
                source = new LocalQueueBridgeSource(ctx, tracePrefix, localName);
            else
                source = new LocalTopicBridgeSource(ctx, tracePrefix, localName, clientId, durName);
            if (remoteType.equals("queue")) {
                sink = new RemoteQueueBridgeSink(ctx, connectionCache.getQueueConnection(remoteFactoryName),
                        connectionCache.getQueue(remoteName));
                session = ((RemoteQueueBridgeSink) sink).getSession();
            } else {
                sink = new RemoteTopicBridgeSink(ctx, connectionCache.getTopicConnection(remoteFactoryName, clientId),
                        connectionCache.getTopic(remoteName));
                session = ((RemoteTopicBridgeSink) sink).getSession();
            }
        } else {
            if (localType.equals("queue"))
                sink = new LocalQueueBridgeSink(ctx, localName);
            else
                sink = new LocalTopicBridgeSink(ctx, localName);
            if (remoteType.equals("queue"))
                source = new RemoteQueueBridgeSource(ctx, tracePrefix,
                        connectionCache.getQueueConnection(remoteFactoryName),
                        connectionCache.getQueue(remoteName));
            else
                source = new RemoteTopicBridgeSource(ctx, tracePrefix,
                        connectionCache.getTopicConnection(remoteFactoryName, clientId),
                        connectionCache.getTopic(remoteName),
                        durName);
        }
        source.setErrorListener(this);
        sink.setPersistenceMode(persistenceMode);
        sink.setCollector(new Collector() {
            public boolean requiresSize() {
                return bridgeCollector != null;
            }

            public void collect(int n, long size) {
                if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "collect, n=" + n + ", size=" + size);
                messagesTransfered += n;
                lastTransferTime = System.currentTimeMillis();
                BridgeCollector c = bridgeCollector;
                if (c != null)
                    c.incTotal(n, size);
            }
        });
        source.setBridgeSink(sink);
        source.startDelivery();
        Thread.currentThread().setContextClassLoader(oldCL);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "createBridge done");
    }

    private void createCollector() {
        String localName = (String) bridgeEntity.getProperty("localname").getValue();
        String localType = (String) bridgeEntity.getProperty("localtype").getValue();
        if (localType.equals("queue"))
            localType = BridgeCollector.DTYPE_QUEUE;
        else
            localType = BridgeCollector.DTYPE_TOPIC;
        String remoteName = (String) bridgeEntity.getProperty("remotename").getValue();
        String remoteType = (String) bridgeEntity.getProperty("remotetype").getValue();
        if (remoteType.equals("queue"))
            remoteType = BridgeCollector.DTYPE_QUEUE;
        else
            remoteType = BridgeCollector.DTYPE_TOPIC;
        String direction = localToRemote ? BridgeCollector.DIRECTION_OUTBOUND : BridgeCollector.DIRECTION_INBOUND;
        bridgeCollector = new BridgeCollector(server.getName(), bridgeEntity.getName(), localName, localType, remoteName, remoteType, direction);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(tracePrefix, "createCollector, bridgeCollector=" + bridgeCollector);
    }

    public void startAccounting(AccountingProfile accountingProfile) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(tracePrefix, "startAccounting, this.accountingProfile=" + this.accountingProfile);
        if (this.accountingProfile == null) {
            this.accountingProfile = accountingProfile;
            createCollector();
        }
    }

    public void flushAccounting() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(tracePrefix, "flushAccounting, accountingProfile=" + accountingProfile + ", bridgeCollector=" + bridgeCollector);
        if (accountingProfile != null && bridgeCollector != null && bridgeCollector.isDirty())
            accountingProfile.getSource().send(bridgeCollector);
    }

    public void stopAccounting() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(tracePrefix, "stopAccounting, accountingProfile=" + accountingProfile);
        if (accountingProfile != null) {
            accountingProfile = null;
            bridgeCollector = null;
        }
    }

    synchronized void collect() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "collect ...");
        try {
            if (messagesTransferedProp != null)
                messagesTransferedProp.setValue(new Integer(messagesTransfered));
            if (lastTransferTimeProp != null && lastTransferTime != -1)
                lastTransferTimeProp.setValue(new Date(lastTransferTime).toString());
        } catch (Exception e) {
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "collect done");
    }

    public void onError(Exception e) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "bridging stopped, exception=" + e);
        ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(BridgeSource.class.getClassLoader());
        if (!server.isDestroyInProgress()) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "initiating server destroy");
            try {
                Thread.sleep(2000);
            } catch (Exception ignored) {
            }
            ctx.bridgeSwiftlet.destroyServer(bridgeEntity.getParent().getParent().getName());
        }
        Thread.currentThread().setContextClassLoader(oldCL);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "bridging stopped");
    }

    synchronized void destroy() {
        if (destroyed)
            return;
        ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(BridgeSource.class.getClassLoader());
        flushAccounting();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "destroying bridge source");
        source.destroy();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "destroying bridge sink");
        sink.destroy();
        Thread.currentThread().setContextClassLoader(oldCL);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, "destroying bridge - finished.");
        destroyed = true;
    }

    public String toString() {
        return "[DestinationBridge, name=" + bridgeEntity.getName() + ", source=" + source + ", sink=" + sink + "]";
    }
}

