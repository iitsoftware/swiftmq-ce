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

package com.swiftmq.impl.accounting.standard.factoryimpl;

import com.swiftmq.impl.accounting.standard.SwiftletContext;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.accounting.*;
import com.swiftmq.swiftlet.queue.QueueManager;

import javax.jms.DeliveryMode;
import java.util.HashMap;
import java.util.Map;

public class QueueSinkFactory implements AccountingSinkFactory {
    SwiftletContext ctx = null;
    Map parms = null;

    public QueueSinkFactory(SwiftletContext ctx) {
        this.ctx = ctx;
        parms = new HashMap();
        Parameter p = new Parameter("Queue Name", "Name of the Sink Queue", null, true, new ParameterVerifier() {
            public void verify(Parameter parameter, String value) throws InvalidValueException {
                QueueManager qm = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
                if (!qm.isQueueRunning(value))
                    throw new InvalidValueException(parameter.getName() + ": Queue '" + value + "' is undefined!");
            }
        });
        parms.put(p.getName(), p);
        p = new Parameter("Delivery Mode", "Either PERSISTENT or NON_PERSISTENT", "PERSISTENT", false, new ParameterVerifier() {
            public void verify(Parameter parameter, String value) throws InvalidValueException {
                String s = value.toUpperCase();
                if (!(s.equals("PERSISTENT") || s.equals("NON_PERSISTENT")))
                    throw new InvalidValueException(parameter.getName() + ": " + value);
            }
        });
        parms.put(p.getName(), p);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), "QueueSinkFactory/created");
    }

    public boolean isSingleton() {
        return false;
    }

    public String getGroup() {
        return "Accounting";
    }

    public String getName() {
        return "QueueSinkFactory";
    }

    public Map getParameters() {
        return parms;
    }

    public AccountingSink create(Map map) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), "QueueSinkFactory/create parameters=" + map);
        String queueName = (String) map.get("Queue Name");
        String deliveryMode = (String) map.get("Delivery Mode");
        if (deliveryMode == null)
            deliveryMode = "PERSISTENT";
        return new QueueSink(ctx, queueName, deliveryMode.equals("PERSISTENT") ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
    }
}
