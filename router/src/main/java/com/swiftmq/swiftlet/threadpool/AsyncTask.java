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

/**
 * An asynchronous task to run in a thread pool.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public interface AsyncTask extends Runnable {
    /**
     * Returns whether the task is valid.
     * This is application dependent. As long as a task stays in the thread pool
     * queue, this state can change. If the task is going to execute, it will be
     * checked whether the task is valid. If not, the task isn't executed anymore.
     * For example, if a task depends on a connection and the connection closes
     * before the task is executed, this method should return false which leads
     * to drop the task out of the pool without executing it.
     *
     * @return true/false.
     */
    public boolean isValid();

    /**
     * Returns the dispatch token of the task.
     * The dispatch token is the thread name, used for thread assignment
     * in the router's configuration file. It is used to determine the pool
     * by invoking <code>dispatchTask</code> of the ThreadpoolSwiftlet directly
     * to determine the pool.
     *
     * @return dispatch token.
     */
    public String getDispatchToken();

    /**
     * Returns a short description of this task.
     * Used for trace outputs.
     *
     * @return description.
     */
    public String getDescription();

    /**
     * Implements the task logic.
     * This method will be called when a task is executed from a pool thread.
     */
    public void run();

    /**
     * Stops this task.
     * This method doesn't stop the task's execution. It is implementation
     * dependent in which state the task will turn. In most cases, the task
     * does some clean up and returns 'false' on <code>isValid()</code> to
     * avoid execution.
     */
    public void stop();
}

