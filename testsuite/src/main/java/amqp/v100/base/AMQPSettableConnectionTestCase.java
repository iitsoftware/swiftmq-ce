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

import com.swiftmq.amqp.v100.client.Connection;
import com.swiftmq.amqp.v100.client.Session;
import junit.framework.TestCase;

public class AMQPSettableConnectionTestCase extends TestCase {
    Connection connection = null;
    Session session = null;

    public AMQPSettableConnectionTestCase(String name) {
        super(name);
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public Connection getConnection() {
        return connection;
    }

    public Session getSession() {
        return session;
    }
}
