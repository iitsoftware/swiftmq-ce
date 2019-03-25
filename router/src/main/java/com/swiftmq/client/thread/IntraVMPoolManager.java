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

package com.swiftmq.client.thread;

import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;

public class IntraVMPoolManager extends PoolManager {
    static final String CONNECTION_TOKEN = "sys$jms.client.connection.%";
    static final String SESSION_TOKEN = "sys$jms.client.session.%";

    ThreadPool connectionPool = null;
    ThreadPool sessionPool = null;

    protected IntraVMPoolManager() {
        ThreadpoolSwiftlet threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
        connectionPool = threadpoolSwiftlet.getPool(CONNECTION_TOKEN);
        sessionPool = threadpoolSwiftlet.getPool(SESSION_TOKEN);
    }

    public synchronized ThreadPool getConnectorPool() {
        return connectionPool;
    }

    public synchronized ThreadPool getConnectionPool() {
        return connectionPool;
    }

    public synchronized ThreadPool getSessionPool() {
        return sessionPool;
    }
}
