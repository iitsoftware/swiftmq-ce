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

import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.tools.prop.SystemProperties;

public class DefaultPoolManager extends PoolManager {
    public static final String PROP_CONNECTOR_POOL_MIN_THREADS = "swiftmq.pool.connector.threads.min";
    public static final String PROP_CONNECTOR_POOL_MAX_THREADS = "swiftmq.pool.connector.threads.max";
    public static final String PROP_CONNECTOR_POOL_PRIO = "swiftmq.pool.connector.priority";
    public static final String PROP_CONNECTOR_POOL_QUEUE_LEN = "swiftmq.pool.connector.queue.length";
    public static final String PROP_CONNECTOR_POOL_THREADS_ADD = "swiftmq.pool.connector.threads.add";
    public static final String PROP_CONNECTOR_POOL_IDLE_TIMEOUT = "swiftmq.pool.connector.idle.timeout";
    public static final String PROP_CONN_POOL_MIN_THREADS = "swiftmq.pool.connection.threads.min";
    public static final String PROP_CONN_POOL_MAX_THREADS = "swiftmq.pool.connection.threads.max";
    public static final String PROP_CONN_POOL_PRIO = "swiftmq.pool.connection.priority";
    public static final String PROP_CONN_POOL_QUEUE_LEN = "swiftmq.pool.connection.queue.length";
    public static final String PROP_CONN_POOL_THREADS_ADD = "swiftmq.pool.connection.threads.add";
    public static final String PROP_CONN_POOL_IDLE_TIMEOUT = "swiftmq.pool.connection.idle.timeout";
    public static final String PROP_SESSION_POOL_MIN_THREADS = "swiftmq.pool.session.threads.min";
    public static final String PROP_SESSION_POOL_MAX_THREADS = "swiftmq.pool.session.threads.max";
    public static final String PROP_SESSION_POOL_PRIO = "swiftmq.pool.session.priority";
    public static final String PROP_SESSION_POOL_QUEUE_LEN = "swiftmq.pool.session.queue.length";
    public static final String PROP_SESSION_POOL_THREADS_ADD = "swiftmq.pool.session.threads.add";
    public static final String PROP_SESSION_POOL_IDLE_TIMEOUT = "swiftmq.pool.session.idle.timeout";

    ThreadPool connectionPool = null;
    ThreadPool sessionPool = null;
    ThreadPool connectorPool = null;

    public synchronized ThreadPool getConnectionPool() {
        if (connectionPool == null) {
            int min = Integer.parseInt(SystemProperties.get(PROP_CONN_POOL_MIN_THREADS, "5"));
            int max = Integer.parseInt(SystemProperties.get(PROP_CONN_POOL_MAX_THREADS, "50"));
            int len = Integer.parseInt(SystemProperties.get(PROP_CONN_POOL_QUEUE_LEN, "1"));
            int add = Integer.parseInt(SystemProperties.get(PROP_CONN_POOL_THREADS_ADD, "1"));
            int prio = Integer.parseInt(SystemProperties.get(PROP_CONN_POOL_PRIO, String.valueOf(Thread.NORM_PRIORITY)));
            long timeout = Long.parseLong(SystemProperties.get(PROP_CONN_POOL_IDLE_TIMEOUT, "120000"));
            connectionPool = new ThreadPoolImpl("ConnectionPool", true, min, max, len, add, prio, timeout);
        }
        return connectionPool;
    }

    public synchronized ThreadPool getSessionPool() {
        if (sessionPool == null) {
            int min = Integer.parseInt(SystemProperties.get(PROP_SESSION_POOL_MIN_THREADS, "5"));
            int max = Integer.parseInt(SystemProperties.get(PROP_SESSION_POOL_MAX_THREADS, "50"));
            int len = Integer.parseInt(SystemProperties.get(PROP_SESSION_POOL_QUEUE_LEN, "1"));
            int add = Integer.parseInt(SystemProperties.get(PROP_SESSION_POOL_THREADS_ADD, "1"));
            int prio = Integer.parseInt(SystemProperties.get(PROP_SESSION_POOL_PRIO, String.valueOf(Thread.NORM_PRIORITY)));
            long timeout = Long.parseLong(SystemProperties.get(PROP_SESSION_POOL_IDLE_TIMEOUT, "120000"));
            sessionPool = new ThreadPoolImpl("SessionPool", true, min, max, len, add, prio, timeout);
        }
        return sessionPool;
    }

    public synchronized ThreadPool getConnectorPool() {
        if (connectorPool == null) {
            int min = Integer.parseInt(SystemProperties.get(PROP_CONNECTOR_POOL_MIN_THREADS, "1"));
            int max = Integer.parseInt(SystemProperties.get(PROP_CONNECTOR_POOL_MAX_THREADS, "10"));
            int len = Integer.parseInt(SystemProperties.get(PROP_CONNECTOR_POOL_QUEUE_LEN, "1"));
            int add = Integer.parseInt(SystemProperties.get(PROP_CONNECTOR_POOL_THREADS_ADD, "1"));
            int prio = Integer.parseInt(SystemProperties.get(PROP_CONNECTOR_POOL_PRIO, String.valueOf(Thread.NORM_PRIORITY)));
            long timeout = Long.parseLong(SystemProperties.get(PROP_CONNECTOR_POOL_IDLE_TIMEOUT, "120000"));
            connectorPool = new ThreadPoolImpl("ConnectorPool", true, min, max, len, add, prio, timeout);
        }
        return connectorPool;
    }
}
