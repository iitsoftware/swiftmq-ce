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
import com.swiftmq.swiftlet.accounting.*;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class AMQPSourceFactory implements AccountingSourceFactory {
    SwiftletContext ctx = null;
    Map parms = null;

    public AMQPSourceFactory(SwiftletContext ctx) {
        this.ctx = ctx;
        this.ctx = ctx;
        parms = new HashMap();
        Parameter p = new Parameter("Flush Interval", "Flush Interval of Accounting Data in milliseconds", "60000", false, new ParameterVerifier() {
            public void verify(Parameter parameter, String value) throws InvalidValueException {
                try {
                    long ms = Long.parseLong(value);
                } catch (NumberFormatException e) {
                    throw new InvalidValueException(e.toString());
                }
            }
        });
        parms.put(p.getName(), p);
        p = new Parameter("User Name Filter", "Regular Expression to filter User Names", null, false, new ParameterVerifier() {
            public void verify(Parameter parameter, String value) throws InvalidValueException {
                try {
                    Pattern.compile(value);
                } catch (Exception e) {
                    throw new InvalidValueException(e.toString());
                }
            }
        });
        parms.put(p.getName(), p);
        p = new Parameter("Client Id Filter", "Regular Expression to filter Client Ids", null, false, new ParameterVerifier() {
            public void verify(Parameter parameter, String value) throws InvalidValueException {
                try {
                    Pattern.compile(value);
                } catch (Exception e) {
                    throw new InvalidValueException(e.toString());
                }
            }
        });
        parms.put(p.getName(), p);
        p = new Parameter("Host Name Filter", "Regular Expression to filter Host Names", null, false, new ParameterVerifier() {
            public void verify(Parameter parameter, String value) throws InvalidValueException {
                try {
                    Pattern.compile(value);
                } catch (Exception e) {
                    throw new InvalidValueException(e.toString());
                }
            }
        });
        parms.put(p.getName(), p);
        p = new Parameter("Queue Name Filter", "Regular Expression to filter Queue Names", null, false, new ParameterVerifier() {
            public void verify(Parameter parameter, String value) throws InvalidValueException {
                try {
                    Pattern.compile(value);
                } catch (Exception e) {
                    throw new InvalidValueException(e.toString());
                }
            }
        });
        parms.put(p.getName(), p);
        p = new Parameter("Topic Name Filter", "Regular Expression to filter Topic Names", null, false, new ParameterVerifier() {
            public void verify(Parameter parameter, String value) throws InvalidValueException {
                try {
                    Pattern.compile(value);
                } catch (Exception e) {
                    throw new InvalidValueException(e.toString());
                }
            }
        });
        parms.put(p.getName(), p);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), "AMQPSourceFactory/created");
    }

    public boolean isSingleton() {
        return true;
    }

    public String getGroup() {
        return "AMQP";
    }

    public String getName() {
        return "AMQPSourceFactory";
    }

    public Map getParameters() {
        return parms;
    }

    public AccountingSource create(Map map) throws Exception {
        long flushInterval = Long.parseLong((String) map.get("Flush Interval"));
        String userNameFilter = (String) map.get("User Name Filter");
        boolean userNameFilterNegate = userNameFilter != null && userNameFilter.startsWith("!");
        if (userNameFilterNegate)
            userNameFilter = userNameFilter.substring(1);
        String clientIdFilter = (String) map.get("Client Id Filter");
        boolean clientIdFilterNegate = clientIdFilter != null && clientIdFilter.startsWith("!");
        if (clientIdFilterNegate)
            clientIdFilter = clientIdFilter.substring(1);
        String hostNameFilter = (String) map.get("Host Name Filter");
        boolean hostNameFilterNegate = hostNameFilter != null && hostNameFilter.startsWith("!");
        if (hostNameFilterNegate)
            hostNameFilter = hostNameFilter.substring(1);
        String queueNameFilter = (String) map.get("Queue Name Filter");
        boolean queueNameFilterNegate = queueNameFilter != null && queueNameFilter.startsWith("!");
        if (queueNameFilterNegate)
            queueNameFilter = queueNameFilter.substring(1);
        String topicNameFilter = (String) map.get("Topic Name Filter");
        boolean topicNameFilterNegate = topicNameFilter != null && topicNameFilter.startsWith("!");
        if (topicNameFilterNegate)
            topicNameFilter = topicNameFilter.substring(1);
        return new AMQPSource(ctx, flushInterval,
                new AccountingProfile(userNameFilter == null ? null : Pattern.compile(userNameFilter), userNameFilterNegate,
                        clientIdFilter == null ? null : Pattern.compile(clientIdFilter), clientIdFilterNegate,
                        hostNameFilter == null ? null : Pattern.compile(hostNameFilter), hostNameFilterNegate,
                        queueNameFilter == null ? null : Pattern.compile(queueNameFilter), queueNameFilterNegate,
                        topicNameFilter == null ? null : Pattern.compile(topicNameFilter), topicNameFilterNegate));
    }
}
