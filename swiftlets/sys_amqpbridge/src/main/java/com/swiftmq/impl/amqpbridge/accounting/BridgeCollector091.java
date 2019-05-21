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

public class BridgeCollector091 extends BridgeCollector {
    private static final String PROP_SOURCECONNECTURI = "sourceconnecturi";
    private static final String PROP_SOURCEEXCHANGENAME = "sourceexchangename";
    private static final String PROP_SOURCEEXCHANGETYPE = "sourceexchangetype";
    private static final String PROP_SOURCECONSUMERTAG = "sourceconsumertag";
    private static final String PROP_SOURCEQUEUENAME = "sourcequeuename";
    private static final String PROP_SOURCEROUTINGKEY = "sourceroutingkey";
    private static final String PROP_TARGETCONNECTURI = "targetconnecturi";
    private static final String PROP_TARGETEXCHANGENAME = "targetexchangename";
    private static final String PROP_TARGETROUTINGKEY = "targetroutingkey";
    private static final String NOT_SET = "not set";

    String sourceConnectURI = null;
    String sourceExchangeName = null;
    String sourceExchangeType = null;
    String sourceConsumerTag = null;
    String sourceQueueName = null;
    String sourceRoutingKey = null;
    String targetConnectURI = null;
    String targetExchangeName = null;
    String targetRoutingKey = null;

    public BridgeCollector091(String bridgeName) {
        super("091", bridgeName);
    }

    public void setSourceConnectURI(String sourceConnectURI) {
        this.sourceConnectURI = sourceConnectURI;
    }

    public void setSourceExchangeName(String sourceExchangeName) {
        this.sourceExchangeName = sourceExchangeName;
    }

    public void setSourceExchangeType(String sourceExchangeType) {
        this.sourceExchangeType = sourceExchangeType;
    }

    public void setSourceConsumerTag(String sourceConsumerTag) {
        this.sourceConsumerTag = sourceConsumerTag;
    }

    public void setSourceQueueName(String sourceQueueName) {
        this.sourceQueueName = sourceQueueName;
    }

    public void setSourceRoutingKey(String sourceRoutingKey) {
        this.sourceRoutingKey = sourceRoutingKey;
    }

    public void setTargetConnectURI(String targetConnectURI) {
        this.targetConnectURI = targetConnectURI;
    }

    public void setTargetExchangeName(String targetExchangeName) {
        this.targetExchangeName = targetExchangeName;
    }

    public void setTargetRoutingKey(String targetRoutingKey) {
        this.targetRoutingKey = targetRoutingKey;
    }

    private String value(String s) {
        if (s == null || s.equals(""))
            return NOT_SET;
        return s;
    }

    public void dumpToMapMessage(MapMessageImpl msg) throws Exception {
        super.dumpToMapMessage(msg);

        msg.setStringProperty(PROP_SOURCECONNECTURI, value(sourceConnectURI));
        msg.setStringProperty(PROP_SOURCEEXCHANGENAME, value(sourceExchangeName));
        msg.setStringProperty(PROP_SOURCEEXCHANGETYPE, value(sourceExchangeType));
        msg.setStringProperty(PROP_SOURCECONSUMERTAG, value(sourceConsumerTag));
        msg.setStringProperty(PROP_SOURCEQUEUENAME, value(sourceQueueName));
        msg.setStringProperty(PROP_SOURCEROUTINGKEY, value(sourceRoutingKey));
        msg.setStringProperty(PROP_TARGETCONNECTURI, value(targetConnectURI));
        msg.setStringProperty(PROP_TARGETEXCHANGENAME, value(targetExchangeName));
        msg.setStringProperty(PROP_TARGETROUTINGKEY, value(targetRoutingKey));
        msg.setString(PROP_SOURCECONNECTURI, value(sourceConnectURI));
        msg.setString(PROP_SOURCEEXCHANGENAME, value(sourceExchangeName));
        msg.setString(PROP_SOURCEEXCHANGETYPE, value(sourceExchangeType));
        msg.setString(PROP_SOURCECONSUMERTAG, value(sourceConsumerTag));
        msg.setString(PROP_SOURCEQUEUENAME, value(sourceQueueName));
        msg.setString(PROP_SOURCEROUTINGKEY, value(sourceRoutingKey));
        msg.setString(PROP_TARGETCONNECTURI, value(targetConnectURI));
        msg.setString(PROP_TARGETEXCHANGENAME, value(targetExchangeName));
        msg.setString(PROP_TARGETROUTINGKEY, value(targetRoutingKey));
    }
}
