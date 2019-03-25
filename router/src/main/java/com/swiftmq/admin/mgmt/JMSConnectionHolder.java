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

package com.swiftmq.admin.mgmt;

import com.swiftmq.jms.ReconnectListener;
import com.swiftmq.jms.SwiftMQConnection;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;

public class JMSConnectionHolder implements ConnectionHolder {
    QueueConnectionFactory connectionFactory = null;
    QueueConnection connection = null;

    public JMSConnectionHolder(QueueConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public JMSConnectionHolder(QueueConnection connection) {
        this.connection = connection;
    }

    public Connection getConnection() {
        return connection;
    }

    public void connect(String username, String password) throws Exception {
        if (connectionFactory != null)
            connection = connectionFactory.createQueueConnection(username, password);
    }

    public void start() throws Exception {
        connection.start();
    }

    public void setExceptionListener(final ExceptionListener listener) throws Exception {
        connection.setExceptionListener(new javax.jms.ExceptionListener() {
            public void onException(JMSException e) {
                listener.onException(e);
            }
        });
    }

    public void addReconnectListener(ReconnectListener listener) {
        ((SwiftMQConnection) connection).addReconnectListener(listener);
    }

    public void removeReconnectListener(ReconnectListener listener) {
        ((SwiftMQConnection) connection).removeReconnectListener(listener);
    }

    public Endpoint createEndpoint(String routerName, RequestServiceFactory rsf, boolean createInternalCommands) throws Exception {
        return EndpointFactory.createEndpoint(routerName, connection, rsf, createInternalCommands);
    }

    public void close() {
        try {
            connection.close();
        } catch (JMSException e) {
        }
    }
}
