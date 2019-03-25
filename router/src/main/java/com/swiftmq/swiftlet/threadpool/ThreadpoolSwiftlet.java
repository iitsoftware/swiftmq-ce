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

package com.swiftmq.swiftlet.threadpool;

import com.swiftmq.swiftlet.Swiftlet;

/**
 * The ThreadpoolSwiftlet manages thread pools for a SwiftMQ router.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2005, All Rights Reserved
 */
public abstract class ThreadpoolSwiftlet extends Swiftlet {
    /**
     * Returns all pool names.
     *
     * @return array of pool names.
     */
    public abstract String[] getPoolnames();

    /**
     * Returns a pool by its name.
     *
     * @param poolName the pool name.
     * @return a thread pool.
     */
    public abstract ThreadPool getPoolByName(String poolName);

    /**
     * Returns a pool for a given thread name.
     * Thread names are assigned to pools in the router configuration
     * file.
     *
     * @param threadName the thread name.
     * @return a thread pool.
     */
    public abstract ThreadPool getPool(String threadName);

    /**
     * Dispatch a task to a pool.
     * This method first determines the pool by the dispatch token of the task and
     * then dispatches the task to the pool. This method is for convinience and
     * should only be used for time-uncritical actions, e.g. if a task is dispatched only
     * once to a pool. Otherwise, the preferred way is to use <code>getPool()</code>
     * once and then dispatch the task directly into that pool.
     *
     * @param asyncTask the task to dispatch.
     */
    public abstract void dispatchTask(AsyncTask asyncTask);

    /**
     * Stops all pools
     */
    public abstract void stopPools();
}

