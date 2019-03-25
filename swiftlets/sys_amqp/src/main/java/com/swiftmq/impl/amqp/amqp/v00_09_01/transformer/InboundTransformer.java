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

package com.swiftmq.impl.amqp.amqp.v00_09_01.transformer;

import com.swiftmq.impl.amqp.amqp.v00_09_01.MessageWrap;
import com.swiftmq.impl.amqp.amqp.v01_00_00.transformer.NameTranslator;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.tools.util.IdGenerator;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import java.util.Map;

public abstract class InboundTransformer {
    static final String PROP_NAME_TRANSLATOR = "name-translator";
    static final String PROP_PREFIX_VENDOR = "prefix-vendor";
    static final String PROP_DEFAULT_DELIVERY_MODE = "default-delivery-mode";
    static final String PROP_DEFAULT_PRIORITY = "default-priority";
    static final String PROP_DEFAULT_TTL = "default-ttl";

    Map config = null;
    NameTranslator nameTranslator = null;
    String prefixVendor = null;
    int defaultDeliveryMode;
    int defaultPriority;
    long defaultTtl;
    String uniqueId = IdGenerator.getInstance().nextId('/');
    long msgId = 0;
    String idPrefix = null;

    protected String nextMsgId() {
        StringBuffer b = new StringBuffer(idPrefix);
        b.append(msgId++);
        return b.toString();
    }

    protected String getValue(String name, String defaultValue) {
        String value = (String) config.get(name);
        if (value == null)
            value = defaultValue;
        return value;
    }

    public void setConfiguration(Map config) throws Exception {
        this.config = config;

        StringBuffer b = new StringBuffer(SwiftletManager.getInstance().getRouterName());
        b.append('/');
        b.append(uniqueId);
        b.append('/');
        idPrefix = b.toString();

        nameTranslator = (NameTranslator) Class.forName(getValue(PROP_NAME_TRANSLATOR, "com.swiftmq.impl.amqp.amqp.v01_00_00.transformer.InvalidToUnderscoreNameTranslator")).newInstance();
        prefixVendor = getValue(PROP_PREFIX_VENDOR, "JMS_AMQP_");
        defaultDeliveryMode = getValue(PROP_DEFAULT_DELIVERY_MODE, "PERSISTENT").toUpperCase().equals("PERSISTENT") ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
        defaultPriority = Integer.parseInt(getValue(PROP_DEFAULT_PRIORITY, String.valueOf(Message.DEFAULT_PRIORITY)));
        defaultTtl = Long.parseLong(getValue(PROP_DEFAULT_TTL, String.valueOf(Message.DEFAULT_TIME_TO_LIVE)));
    }

    public abstract MessageImpl transform(MessageWrap messageWrap, DestinationFactory destinationFactory) throws JMSException;

}
