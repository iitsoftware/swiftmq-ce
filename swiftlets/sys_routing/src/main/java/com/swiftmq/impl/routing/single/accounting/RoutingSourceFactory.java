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
import com.swiftmq.swiftlet.accounting.*;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class RoutingSourceFactory implements AccountingSourceFactory {
    SwiftletContext ctx = null;
    Map parms = null;

    public RoutingSourceFactory(SwiftletContext ctx) {
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
        p = new Parameter("Source Router Filter", "Regular Expression to filter Source Router Names", null, false, new ParameterVerifier() {
            public void verify(Parameter parameter, String value) throws InvalidValueException {
                try {
                    Pattern.compile(value);
                } catch (Exception e) {
                    throw new InvalidValueException(e.toString());
                }
            }
        });
        parms.put(p.getName(), p);
        p = new Parameter("Destination Router Filter", "Regular Expression to filter Destination Router Names", null, false, new ParameterVerifier() {
            public void verify(Parameter parameter, String value) throws InvalidValueException {
                try {
                    Pattern.compile(value);
                } catch (Exception e) {
                    throw new InvalidValueException(e.toString());
                }
            }
        });
        parms.put(p.getName(), p);
        p = new Parameter("Connected Router Filter", "Regular Expression to filter Connected Router Names", null, false, new ParameterVerifier() {
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
        p = new Parameter("Direction", "States which routing direction should be accounted: inbound, outbound, or both", "both", false, new ParameterVerifier() {
            public void verify(Parameter parameter, String value) throws InvalidValueException {
                String s = value.toLowerCase();
                if (!(s.equals("inbound") || s.equals("outbound") || s.equals("both")))
                    throw new InvalidValueException("Invalid value '" + value + "': inbound, outbound, or both expected");
            }
        });
        parms.put(p.getName(), p);
    }

    public boolean isSingleton() {
        return true;
    }

    public String getGroup() {
        return "Routing";
    }

    public String getName() {
        return "RoutingSourceFactory";
    }

    public Map getParameters() {
        return parms;
    }

    public AccountingSource create(Map map) throws Exception {
        long flushInterval = Long.parseLong((String) map.get("Flush Interval"));
        String sourceRouterFilter = (String) map.get("Source Router Filter");
        boolean sourceRouterFilterNegate = sourceRouterFilter != null && sourceRouterFilter.startsWith("!");
        if (sourceRouterFilterNegate)
            sourceRouterFilter = sourceRouterFilter.substring(1);
        String destRouterFilter = (String) map.get("Destination Router Filter");
        boolean destRouterFilterNegate = destRouterFilter != null && destRouterFilter.startsWith("!");
        if (destRouterFilterNegate)
            destRouterFilter = destRouterFilter.substring(1);
        String connectedRouterFilter = (String) map.get("Connected Router Filter");
        boolean connectedRouterFilterNegate = connectedRouterFilter != null && connectedRouterFilter.startsWith("!");
        if (connectedRouterFilterNegate)
            connectedRouterFilter = connectedRouterFilter.substring(1);
        String queueNameFilter = (String) map.get("Queue Name Filter");
        boolean queueNameFilterNegate = queueNameFilter != null && queueNameFilter.startsWith("!");
        if (queueNameFilterNegate)
            queueNameFilter = queueNameFilter.substring(1);
        String topicNameFilter = (String) map.get("Topic Name Filter");
        boolean topicNameFilterNegate = topicNameFilter != null && topicNameFilter.startsWith("!");
        if (topicNameFilterNegate)
            topicNameFilter = topicNameFilter.substring(1);
        String direction = ((String) map.get("Direction")).toLowerCase();
        return new RoutingSource(ctx, flushInterval,
                new AccountingProfile(sourceRouterFilter == null ? null : Pattern.compile(sourceRouterFilter), sourceRouterFilterNegate,
                        destRouterFilter == null ? null : Pattern.compile(destRouterFilter), destRouterFilterNegate,
                        connectedRouterFilter == null ? null : Pattern.compile(connectedRouterFilter), connectedRouterFilterNegate,
                        queueNameFilter == null ? null : Pattern.compile(queueNameFilter), queueNameFilterNegate,
                        topicNameFilter == null ? null : Pattern.compile(topicNameFilter), topicNameFilterNegate,
                        direction.equals("inbound") || direction.equals("both"),
                        direction.equals("outbound") || direction.equals("both")));

    }
}
