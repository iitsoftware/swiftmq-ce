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
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.accounting.*;
import com.swiftmq.swiftlet.queue.QueueManager;

import javax.jms.InvalidSelectorException;
import java.util.HashMap;
import java.util.Map;

public class QueueSourceFactory implements AccountingSourceFactory {
    SwiftletContext ctx = null;
    Map parms = null;

    public QueueSourceFactory(SwiftletContext ctx) {
        this.ctx = ctx;
        parms = new HashMap();
        Parameter p = new Parameter("Queue Name", "Name of the Source Queue", null, true, new ParameterVerifier() {
            public void verify(Parameter parameter, String value) throws InvalidValueException {
                QueueManager qm = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
                if (!qm.isQueueDefined(value))
                    throw new InvalidValueException(parameter.getName() + ": Queue '" + value + "' is undefined!");
            }
        });
        parms.put(p.getName(), p);
        p = new Parameter("Selector", "Optional Message Selector", null, false, new ParameterVerifier() {
            public void verify(Parameter parameter, String value) throws InvalidValueException {
                if (value != null) {
                    MessageSelector sel = new MessageSelector(value);
                    try {
                        sel.compile();
                    } catch (InvalidSelectorException e) {
                        throw new InvalidValueException(parameter.getName() + ": " + e.getMessage());
                    }
                }
            }
        });
        parms.put(p.getName(), p);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), "QueueSourceFactory/created");
    }

    public boolean isSingleton() {
        return false;
    }

    public String getGroup() {
        return "Accounting";
    }

    public String getName() {
        return "QueueSourceFactory";
    }

    public Map getParameters() {
        return parms;
    }

    public AccountingSource create(Map map) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), "QueueSourceFactory/create parameters=" + map);
        String queueName = (String) map.get("Queue Name");
        String selector = (String) map.get("Selector");
        MessageSelector ms = null;
        if (selector != null) {
            ms = new MessageSelector(selector);
            ms.compile();
        }
        return new QueueSource(ctx, queueName, ms);
    }
}
