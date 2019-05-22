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

package com.swiftmq.extension.jmsbridge;

import javax.jms.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ConnectionCache {
    ObjectFactory objectFactory = null;
    String userName = null;
    String password = null;
    ExceptionListener exceptionListener = null;
    Map qcFactoryCache = new HashMap();
    Map tcFactoryCache = new HashMap();
    Map qcCache = new HashMap();
    Map tcCache = new HashMap();

    ConnectionCache(ObjectFactory objectFactory, String userName, String password, ExceptionListener exceptionListener) {
        this.objectFactory = objectFactory;
        this.userName = userName;
        this.password = password;
        this.exceptionListener = exceptionListener;
        /*${evaltimer}*/
    }

    synchronized QueueConnection getQueueConnection(String factoryName)
            throws Exception {
        QueueConnectionFactory qcf = (QueueConnectionFactory) qcFactoryCache.get(factoryName);
        QueueConnection qc = null;
        if (qcf == null) {
            qcf = objectFactory.getQueueConnectionFactory(factoryName);
            qcFactoryCache.put(factoryName, qcf);
            if (userName == null || userName.trim().length() == 0)
                qc = qcf.createQueueConnection();
            else
                qc = qcf.createQueueConnection(userName, password);
            qcCache.put(factoryName, qc);
            if (exceptionListener != null)
                qc.setExceptionListener(exceptionListener);
            qc.start();
        } else
            qc = (QueueConnection) qcCache.get(factoryName);
        return qc;
    }

    synchronized TopicConnection getTopicConnection(String factoryName, String clientId)
            throws Exception {
        TopicConnectionFactory tcf = (TopicConnectionFactory) tcFactoryCache.get(factoryName);
        TopicConnection tc = null;
        if (tcf == null) {
            tcf = objectFactory.getTopicConnectionFactory(factoryName);
            tcFactoryCache.put(factoryName, tcf);
            if (userName == null || userName.trim().length() == 0)
                tc = tcf.createTopicConnection();
            else
                tc = tcf.createTopicConnection(userName, password);
            tcCache.put(factoryName, tc);
            if (clientId != null)
                tc.setClientID(clientId);
            if (exceptionListener != null)
                tc.setExceptionListener(exceptionListener);
            tc.start();
        } else
            tc = (TopicConnection) tcCache.get(factoryName);
        return tc;
    }

    synchronized Queue getQueue(String queueName) throws Exception {
        return (Queue) objectFactory.getQueue(queueName);

    }

    synchronized Topic getTopic(String topicName) throws Exception {
        return (Topic) objectFactory.getTopic(topicName);
    }

    synchronized void closeAll() {
        for (Iterator iter = qcCache.entrySet().iterator(); iter.hasNext(); ) {
            QueueConnection qc = (QueueConnection) ((Map.Entry) iter.next()).getValue();
            try {
                qc.close();
            } catch (Exception ignored) {
            }
        }
        qcCache.clear();
        for (Iterator iter = tcCache.entrySet().iterator(); iter.hasNext(); ) {
            TopicConnection tc = (TopicConnection) ((Map.Entry) iter.next()).getValue();
            try {
                tc.close();
            } catch (Exception ignored) {
            }
        }
        tcCache.clear();
        qcFactoryCache.clear();
        tcFactoryCache.clear();
    }
}

