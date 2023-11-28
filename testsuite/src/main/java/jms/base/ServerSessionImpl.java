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

package jms.base;

import javax.jms.JMSException;
import javax.jms.ServerSession;
import javax.jms.Session;

public class ServerSessionImpl
        implements ServerSession {
    ServerSessionPoolImpl pool = null;
    Session session = null;

    public ServerSessionImpl(ServerSessionPoolImpl pool, Session session) {
        this.pool = pool;
        this.session = session;
    }

    public Session getSession() throws JMSException {
        return session;
    }

    public void start() throws JMSException {
        pool.getExecutorService().execute(new Runnable() {
            public void run() {
                session.run();
                pool.addServerSession(ServerSessionImpl.this);
            }
        });
    }
}
