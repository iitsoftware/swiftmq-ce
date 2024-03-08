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

import com.swiftmq.amqp.v100.client.Session;

public class AMQPConnectedSessionTestCase extends AMQPConnectedTestCase {
    Session session = null;

    public AMQPConnectedSessionTestCase(String name) {
        super(name);
    }

    public Session getSession() {
        return session;
    }

    protected void setUp() throws Exception {
        super.setUp();
        session = Util.createSession(getConnection());
    }

    protected void tearDown() throws Exception {
        if (session != null)
            session.close();
        super.tearDown();
    }
}
