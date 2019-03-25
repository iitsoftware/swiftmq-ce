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

package com.swiftmq.impl.amqp.amqp.v01_00_00.transformer;

import com.swiftmq.amqp.v100.client.AMQPException;
import com.swiftmq.impl.amqp.amqp.v01_00_00.Delivery;

import javax.jms.JMSException;
import java.util.Map;

public abstract class OutboundTransformer {
    static final String PROP_NAME_TRANSLATOR = "name-translator";
    static final String PROP_PREFIX_VENDOR = "prefix-vendor";

    Map config = null;
    NameTranslator nameTranslator = null;
    String prefixVendor = null;
    String amqpNative;
    String messageFormat;

    protected String getValue(String name, String defaultValue) {
        String value = (String) config.get(name);
        if (value == null)
            value = defaultValue;
        return value;
    }

    public void setConfiguration(Map config) throws Exception {
        this.config = config;
        nameTranslator = (NameTranslator) Class.forName(getValue(PROP_NAME_TRANSLATOR, "com.swiftmq.impl.amqp.amqp.v01_00_00.transformer.NullNameTranslator")).newInstance();
        prefixVendor = getValue(PROP_PREFIX_VENDOR, "JMS_AMQP_");
        amqpNative = prefixVendor + Util.PROP_AMQP_NATIVE;
        messageFormat = prefixVendor + Util.PROP_MESSAGE_FORMAT;
    }

    public abstract void transform(Delivery delivery) throws AMQPException, JMSException;

}
