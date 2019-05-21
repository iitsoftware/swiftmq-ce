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

package com.swiftmq.impl.amqpbridge.accounting;

import com.swiftmq.jms.MapMessageImpl;

public class BridgeCollector100 extends BridgeCollector {
    private static final String PROP_SOURCEHOST = "sourcehost";
    private static final String PROP_SOURCEPORT = "sourceport";
    private static final String PROP_SOURCEADDRESS = "sourceaddress";
    private static final String PROP_TARGETHOST = "targethost";
    private static final String PROP_TARGETPORT = "targetport";
    private static final String PROP_TARGETADDRESS = "targetaddress";
    private static final String NOT_SET = "not set";

    String sourceHost = null;
    String sourcePort = null;
    String sourceAddress = null;
    String targetHost = null;
    String targetPort = null;
    String targetAddress = null;

    public BridgeCollector100(String bridgeName) {
        super("100", bridgeName);
    }

    public void setSourceHost(String sourceHost) {
        this.sourceHost = sourceHost;
    }

    public void setSourcePort(String sourcePort) {
        this.sourcePort = sourcePort;
    }

    public void setSourceAddress(String sourceAddress) {
        this.sourceAddress = sourceAddress;
    }

    public void setTargetHost(String targetHost) {
        this.targetHost = targetHost;
    }

    public void setTargetPort(String targetPort) {
        this.targetPort = targetPort;
    }

    public void setTargetAddress(String targetAddress) {
        this.targetAddress = targetAddress;
    }

    private String value(String s) {
        if (s == null || s.equals(""))
            return NOT_SET;
        return s;
    }

    public void dumpToMapMessage(MapMessageImpl msg) throws Exception {
        super.dumpToMapMessage(msg);

        msg.setStringProperty(PROP_SOURCEHOST, value(sourceHost));
        msg.setStringProperty(PROP_SOURCEPORT, value(sourcePort));
        msg.setStringProperty(PROP_SOURCEADDRESS, value(sourceAddress));
        msg.setStringProperty(PROP_TARGETHOST, value(targetHost));
        msg.setStringProperty(PROP_TARGETPORT, value(targetPort));
        msg.setStringProperty(PROP_TARGETADDRESS, value(targetAddress));
        msg.setString(PROP_SOURCEHOST, value(sourceHost));
        msg.setString(PROP_SOURCEPORT, value(sourcePort));
        msg.setString(PROP_SOURCEADDRESS, value(sourceAddress));
        msg.setString(PROP_TARGETHOST, value(targetHost));
        msg.setString(PROP_TARGETPORT, value(targetPort));
        msg.setString(PROP_TARGETADDRESS, value(targetAddress));
    }
}
