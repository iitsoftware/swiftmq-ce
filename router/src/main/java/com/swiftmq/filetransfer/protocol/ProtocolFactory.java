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

package com.swiftmq.filetransfer.protocol;

import javax.jms.JMSException;
import javax.jms.Message;

public class ProtocolFactory implements MessageBasedFactory {
    public static final String DUMPID_PROP = "JMS_SWIFTMQ_FT_DUMPID";
    public static final int PROTOCOL_REQ = 0;
    public static final int PROTOCOL_REP = 1;

    MessageBasedFactory delegatedFactory = null;

    public ProtocolFactory() {
    }

    public ProtocolFactory(MessageBasedFactory delegatedFactory) {
        this.delegatedFactory = delegatedFactory;
    }

    public void setDelegatedFactory(MessageBasedFactory delegatedFactory) {
        this.delegatedFactory = delegatedFactory;
    }

    public MessageBased create(Message message) throws JMSException {
        MessageBased messageBased = null;

        int dumpId = message.getIntProperty(DUMPID_PROP);
        switch (dumpId) {
            case PROTOCOL_REQ:
                messageBased = new ProtocolRequest(message);
                break;
            case PROTOCOL_REP:
                messageBased = new ProtocolReply(message);
                break;
            default:
                if (delegatedFactory != null)
                    messageBased = delegatedFactory.create(message);
                break;
        }
        return messageBased;
    }
}
