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

package com.swiftmq.impl.threadpool.standard;

import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.threadpool.AsyncTask;

public class PoolThread extends Thread
{
  PoolDispatcher poolDispatcher;
  long idleTimeout;
  AsyncTask activeTask;
  volatile boolean shouldDie = false;

  PoolThread(String name, ThreadGroup threadGroup, PoolDispatcher poolDispatcher, long idleTimeout)
  {
    super(threadGroup, name);
    this.poolDispatcher = poolDispatcher;
    this.idleTimeout = idleTimeout;
  }

  public AsyncTask getActiveTask()
  {
    return (activeTask);
  }

  void die()
  {
    shouldDie = true;
    if (activeTask != null)
      activeTask.stop();
  }

  public void run()
  {
    activeTask = poolDispatcher.getNextTask(this, idleTimeout, activeTask);
    while (activeTask != null && !shouldDie)
    {
      if (activeTask.isValid())
      {
        try
        {
          activeTask.run();
        } catch (OutOfMemoryError oom)
        {
          System.err.println("Got OutOfMemoryError:");
          System.err.println("    ThreadGroup: " + getThreadGroup().getName());
          System.err.println("    ActiveTask : " + activeTask.getDescription());
          System.err.println("Stack Trace: ");
          oom.printStackTrace();
          if (SwiftletManager.getInstance().isHA())
          {
            System.err.println("Try to shutdown this HA instance to force a failover.");
            SwiftletManager.getInstance().disableShutdownHook();
            System.exit(-1);
          }
        } catch (Throwable e)
        {
          System.err.println("Got Exception:");
          System.err.println("    ThreadGroup: " + getThreadGroup().getName());
          System.err.println("    ActiveTask : " + activeTask.getDescription());
          System.err.println("Stack Trace: ");
          e.printStackTrace();
        }
      }
      activeTask = null; // to enable GC of the task objects
      if (!shouldDie)
        activeTask = poolDispatcher.getNextTask(this, idleTimeout, activeTask);
    }
  }
}

