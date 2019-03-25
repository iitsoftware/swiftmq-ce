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

import com.swiftmq.swiftlet.threadpool.event.FreezeCompletionListener;

/**
 * A thread pool.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public interface ThreadPool {

    /**
     * Returns the pool name.
     *
     * @return pool name.
     */
    public String getPoolName();

    /**
     * Returns the number of currently idling threads.
     * Used from management tools only.
     *
     * @return number of idling threads.
     */
    public int getNumberIdlingThreads();

    /**
     * Returns the number of currently running threads.
     * Used from management tools only.
     *
     * @return number of running threads.
     */
    public int getNumberRunningThreads();

    /**
     * Dispatch a task into the pool.
     *
     * @param asyncTask the task to dispatch.
     */
    public void dispatchTask(AsyncTask asyncTask);

    /**
     * Freezes this pool. That is, the current running tasks are completed but
     * no further tasks will be scheduled until unfreeze() is called. It is possible
     * to dispatch tasks during freeze. However, these will be executed after unfreeze()
     * is called.
     *
     * @param listener will be called when the pool is freezed.
     */
    public void freeze(FreezeCompletionListener listener);

    /**
     * Unfreezes this pool.
     */
    public void unfreeze();

    /**
     * Stops the pool.
     * Internal use only.
     */
    public void stop();

    /**
     * Closes the pool.
     * Internal use only.
     */
    public void close();
}

