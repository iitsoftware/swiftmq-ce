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

package amqp.v100.base;

import com.swiftmq.amqp.AMQPContext;
import com.swiftmq.amqp.v100.client.Connection;
import com.swiftmq.amqp.v100.client.Session;
import com.swiftmq.net.SocketFactory;
import junit.framework.TestCase;

public class Util {
    public static Connection createConnection() throws Exception {
        return createConnection(null);
    }

    public static Connection createConnection(String containerId) throws Exception {
        AMQPContext ctx = new AMQPContext(AMQPContext.CLIENT);
        Connection connection = null;
        SocketFactory socketFactory = null;
        if (System.getProperty("socketfactory") != null)
            socketFactory = (SocketFactory) Class.forName(System.getProperty("socketfactory")).newInstance();
        String hostname = System.getProperty("hostname", "localhost");
        int port = Integer.parseInt(System.getProperty("port", "5672"));
        boolean useSasl = Boolean.parseBoolean(System.getProperty("usesasl", "false"));
        if (useSasl) {
            boolean anonLogin = Boolean.parseBoolean(System.getProperty("anonlogin", "false"));
            if (anonLogin) {
                connection = new Connection(ctx, hostname, port, true);
            } else {
                String username = System.getProperty("username");
                TestCase.assertNotNull("missing property 'username'", username);
                String password = System.getProperty("password");
                TestCase.assertNotNull("missing property 'password'", password);
                connection = new Connection(ctx, hostname, port, username, password);
                String mechanism = System.getProperty("mechanism", "PLAIN");
                connection.setMechanism(mechanism);
            }
        } else {
            connection = new Connection(ctx, hostname, port, false);
        }
        if (containerId != null)
            connection.setContainerId(containerId);
        if (socketFactory != null)
            connection.setSocketFactory(socketFactory);
        long idleTimeout = Long.parseLong(System.getProperty("idletimeout", "60000"));
        if (idleTimeout > 0)
            connection.setIdleTimeout(idleTimeout);
        if (System.getProperty("maxframesize") != null)
            connection.setMaxFrameSize(Long.parseLong(System.getProperty("maxframesize")));
        connection.connect();
        return connection;
    }

    public static Session createSession(Connection connection) throws Exception {
        long windowIn = Long.parseLong(System.getProperty("session.window.incoming", "100"));
        long windowOut = Long.parseLong(System.getProperty("session.window.outgoing", "100"));
        return connection.createSession(windowIn, windowOut);
    }

    public static String getQueueNamePrefix() {
        return System.getProperty("prefix-queuename", "amqp_testqueue");
    }

    public static String getTopicNamePrefix() {
        return System.getProperty("prefix-topicname", "amqp_testtopic");
    }
}
