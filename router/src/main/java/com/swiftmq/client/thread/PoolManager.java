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

public abstract class PoolManager {
    private static PoolManager _instance = null;

    protected PoolManager() {
    }

    public static synchronized void reset() {
        _instance = null;
    }

    public static synchronized void setIntraVM(boolean intraVM) {
        if (_instance == null) {
            if (intraVM)
                _instance = new IntraVMPoolManager();
            else
                _instance = new DefaultPoolManager();
        }
    }

    public static synchronized PoolManager getInstance() {
        return _instance;
    }

    public abstract ThreadPool getConnectorPool();

    public abstract ThreadPool getConnectionPool();

    public abstract ThreadPool getSessionPool();
}
