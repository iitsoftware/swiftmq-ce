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

package com.swiftmq.jms.v400;

import javax.jms.JMSException;
import javax.jms.XASession;
import javax.jms.XATopicConnection;
import javax.jms.XATopicSession;

public class XATopicConnectionImpl extends TopicConnectionImpl
        implements XATopicConnection {

    protected XATopicConnectionImpl(String userName, String password, com.swiftmq.net.client.Connection conn)
            throws JMSException {
        super(userName, password, conn);
    }

    public XATopicSession createXATopicSession() throws JMSException {
        return new XATopicSessionImpl((SessionImpl) createTopicSession(true, 0));
    }

    public XASession createXASession() throws JMSException {
        return new XASessionImpl((SessionImpl) createSession(true, 0));
    }
}
