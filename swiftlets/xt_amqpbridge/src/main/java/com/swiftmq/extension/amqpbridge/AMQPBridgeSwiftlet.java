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

package com.swiftmq.extension.amqpbridge;

import com.swiftmq.extension.amqpbridge.accounting.AMQPBridgeSourceFactory;
import com.swiftmq.extension.amqpbridge.accounting.AccountingProfile;
import com.swiftmq.extension.amqpbridge.jobs.JobRegistrar;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.Swiftlet;
import com.swiftmq.swiftlet.SwiftletException;

public class AMQPBridgeSwiftlet extends Swiftlet {
    SwiftletContext ctx = null;
    EntityListEventAdapter amqpbridge100Adapter = null;
    EntityListEventAdapter amqpbridge091Adapter = null;
    JobRegistrar jobRegistrar = null;
    AMQPBridgeSourceFactory sourceFactory = null;

    public void startAccounting(String bridgeType, String bridgeName, AccountingProfile accountingProfile) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "startAccounting, bridgeType=" + bridgeType + ", bridgeName=" + bridgeName + ", accountingfProfile=" + accountingProfile);
        if (bridgeType.equals("091")) {
            Entity bridgeEntity = ctx.root.getEntity("bridges-091").getEntity(bridgeName);
            if (bridgeEntity != null) {
                com.swiftmq.extension.amqpbridge.v091.BridgeController bridgeController = (com.swiftmq.extension.amqpbridge.v091.BridgeController) bridgeEntity.getUserObject();
                if (bridgeController != null)
                    bridgeController.startAccounting(accountingProfile);
            }
        } else {
            Entity bridgeEntity = ctx.root.getEntity("bridges-100").getEntity(bridgeName);
            if (bridgeEntity != null) {
                com.swiftmq.extension.amqpbridge.v100.BridgeController bridgeController = (com.swiftmq.extension.amqpbridge.v100.BridgeController) bridgeEntity.getUserObject();
                if (bridgeController != null)
                    bridgeController.startAccounting(accountingProfile);
            }
        }
    }

    public void stopAccounting(String bridgeType, String bridgeName) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "stopAccounting, bridgeType=" + bridgeType + ", bridgeName=" + bridgeName);
        if (bridgeType.equals("091")) {
            Entity bridgeEntity = ctx.root.getEntity("bridges-091").getEntity(bridgeName);
            if (bridgeEntity != null) {
                com.swiftmq.extension.amqpbridge.v091.BridgeController bridgeController = (com.swiftmq.extension.amqpbridge.v091.BridgeController) bridgeEntity.getUserObject();
                if (bridgeController != null)
                    bridgeController.stopAccounting();
            }
        } else {
            Entity bridgeEntity = ctx.root.getEntity("bridges-100").getEntity(bridgeName);
            if (bridgeEntity != null) {
                com.swiftmq.extension.amqpbridge.v100.BridgeController bridgeController = (com.swiftmq.extension.amqpbridge.v100.BridgeController) bridgeEntity.getUserObject();
                if (bridgeController != null)
                    bridgeController.stopAccounting();
            }
        }
    }

    public void flushAccounting(String bridgeType, String bridgeName) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "flushAccounting, bridgeType=" + bridgeType + ", bridgeName=" + bridgeName);
        if (bridgeType.equals("091")) {
            Entity bridgeEntity = ctx.root.getEntity("bridges-091").getEntity(bridgeName);
            if (bridgeEntity != null) {
                com.swiftmq.extension.amqpbridge.v091.BridgeController bridgeController = (com.swiftmq.extension.amqpbridge.v091.BridgeController) bridgeEntity.getUserObject();
                if (bridgeController != null)
                    bridgeController.flushAccounting();
            }
        } else {
            Entity bridgeEntity = ctx.root.getEntity("bridges-100").getEntity(bridgeName);
            if (bridgeEntity != null) {
                com.swiftmq.extension.amqpbridge.v100.BridgeController bridgeController = (com.swiftmq.extension.amqpbridge.v100.BridgeController) bridgeEntity.getUserObject();
                if (bridgeController != null)
                    bridgeController.flushAccounting();
            }
        }
    }

    private void createAMQP100Adapter(EntityList list) throws SwiftletException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createAMQP100Adapter ...");
        amqpbridge100Adapter = new EntityListEventAdapter(list, true, true) {
            public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd: " + newEntity.getName() + " ...");
                try {
                    com.swiftmq.extension.amqpbridge.v100.BridgeController bridgeController = new com.swiftmq.extension.amqpbridge.v100.BridgeController(ctx, newEntity);
                    newEntity.setUserObject(bridgeController);
                } catch (Exception e) {
                    throw new EntityAddException(e.toString());
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd: " + newEntity.getName() + " done");
            }

            public void onEntityRemove(Entity parent, Entity delEntity) throws EntityRemoveException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove: " + delEntity.getName() + " ...");
                com.swiftmq.extension.amqpbridge.v100.BridgeController bridgeController = (com.swiftmq.extension.amqpbridge.v100.BridgeController) delEntity.getUserObject();
                bridgeController.close();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove: " + delEntity.getName() + " done");
            }
        };
        try {
            amqpbridge100Adapter.init();
        } catch (Exception e) {
            throw new SwiftletException(e.toString());
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createAMQP100Adapter done");
    }

    private void createAMQP091Adapter(EntityList list) throws SwiftletException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createAMQP091Adapter ...");
        amqpbridge091Adapter = new EntityListEventAdapter(list, true, true) {
            public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd: " + newEntity.getName() + " ...");
                try {
                    com.swiftmq.extension.amqpbridge.v091.BridgeController bridgeController = new com.swiftmq.extension.amqpbridge.v091.BridgeController(ctx, newEntity);
                    newEntity.setUserObject(bridgeController);
                } catch (Exception e) {
                    throw new EntityAddException(e.toString());
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd: " + newEntity.getName() + " done");
            }

            public void onEntityRemove(Entity parent, Entity delEntity) throws EntityRemoveException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove: " + delEntity.getName() + " ...");
                com.swiftmq.extension.amqpbridge.v091.BridgeController bridgeController = (com.swiftmq.extension.amqpbridge.v091.BridgeController) delEntity.getUserObject();
                bridgeController.close();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove: " + delEntity.getName() + " done");
            }
        };
        try {
            amqpbridge091Adapter.init();
        } catch (Exception e) {
            throw new SwiftletException(e.toString());
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createAMQP091Adapter done");
    }

    protected void startup(Configuration config) throws SwiftletException {
        ctx = new SwiftletContext(config, this);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup ...");
        /*${evalstartupmark}*/

        createAMQP100Adapter((EntityList) ctx.root.getEntity("bridges100"));
        createAMQP091Adapter((EntityList) ctx.root.getEntity("bridges091"));

        jobRegistrar = new JobRegistrar(ctx);
        jobRegistrar.register();
        if (ctx.accountingSwiftlet != null) {
            sourceFactory = new AMQPBridgeSourceFactory(ctx);
            ctx.accountingSwiftlet.addAccountingSourceFactory(sourceFactory.getGroup(), sourceFactory.getName(), sourceFactory);
        }

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup done.");
    }

    protected void shutdown() throws SwiftletException {
        if (ctx == null)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown ...");
        jobRegistrar.unregister();
        jobRegistrar = null;

        try {
            amqpbridge100Adapter.close();
        } catch (Exception e) {
        }
        try {
            amqpbridge091Adapter.close();
        } catch (Exception e) {
        }
        if (ctx.accountingSwiftlet != null) {
            ctx.accountingSwiftlet.removeAccountingSourceFactory(sourceFactory.getGroup(), sourceFactory.getName());
            sourceFactory = null;
        }
        ctx.close();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown done.");
    }
}
