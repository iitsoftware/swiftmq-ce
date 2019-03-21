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
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.swiftlet.threadpool.event.FreezeCompletionListener;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;
import com.swiftmq.tools.collection.RingBuffer;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PoolDispatcher implements ThreadPool
{
  private final static int BUCKET_SIZE = 200;

  TraceSwiftlet traceSwiftlet = null;
  TraceSpace traceSpace = null;
  String tracePrefix = null;

  String poolName;
  ThreadGroup threadGroup;
  boolean kernelPool = false;
  int minThreads;
  int maxThreads;
  int threshold;
  int addThreads;
  int priority;
  long idleTimeout;
  Set threadSet = new HashSet();
  int runningCount = 0;
  int idleCount = 0;
  boolean stopped = false;
  boolean closed = false;
  RingBuffer taskList = null;
  FreezeCompletionListener freezeCompletionListener = null;
  boolean freezed = false;
  int tcount = 0;
  String tname = null;
  Lock lock = new ReentrantLock();
  Condition taskAvail = null;

  PoolDispatcher(String tracePrefix, String poolName, boolean kernelPool, int minThreads, int maxThreads, int threshold, int addThreads, int priority, long idleTimeout)
  {
    traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
    traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
    this.tracePrefix = tracePrefix + "/" + poolName;
    if (traceSpace.enabled) traceSpace.trace(this.tracePrefix, "initializing");
    taskAvail = lock.newCondition();

    this.kernelPool = kernelPool;
    this.poolName = poolName;
    this.minThreads = minThreads;
    this.maxThreads = maxThreads;
    this.threshold = threshold;
    this.addThreads = addThreads;
    this.priority = priority;
    this.idleTimeout = idleTimeout;
    tname = "SwiftMQ-" + poolName + "-";
    threadGroup = new ThreadGroup(poolName);
    threadGroup.setMaxPriority(priority);
    taskList = new RingBuffer(BUCKET_SIZE);
    for (int i = 0; i < minThreads; i++)
      createNewThread(-1);
  }

  private void createNewThread(long threadTimeout)
  {
    if (traceSpace.enabled) traceSpace.trace(tracePrefix, "createNewThread, threadTimeout=" + threadTimeout);
    PoolThread pt = new PoolThread(tname + (++tcount), threadGroup, this, threadTimeout);
    runningCount++;
    threadSet.add(pt);
    pt.start();
  }

  public String getPoolName()
  {
    return poolName;
  }

  public void setKernelPool(boolean b)
  {
    lock.lock();
    try
    {
      kernelPool = b;
    } finally
    {
      lock.unlock();
    }
  }

  public boolean isKernelPool()
  {
    lock.lock();
    try
    {
      return kernelPool;
    } finally
    {
      lock.unlock();
    }
  }

  public int getMinThreads()
  {
    lock.lock();
    try
    {
      return minThreads;
    } finally
    {
      lock.unlock();
    }
  }

  public void setMinThreads(int minThreads)
  {
    lock.lock();
    try
    {
      this.minThreads = minThreads;
    } finally
    {
      lock.unlock();
    }
  }

  public int getMaxThreads()
  {
    lock.lock();
    try
    {
      return maxThreads;
    } finally
    {
      lock.unlock();
    }
  }

  public void setMaxThreads(int maxThreads)
  {
    lock.lock();
    try
    {
      this.maxThreads = maxThreads;
    } finally
    {
      lock.unlock();
    }
  }

  public int getThreshold()
  {
    lock.lock();
    try
    {
      return threshold;
    } finally
    {
      lock.unlock();
    }
  }

  public void setThreshold(int threshold)
  {
    lock.lock();
    try
    {
      this.threshold = threshold;
    } finally
    {
      lock.unlock();
    }
  }

  public int getAddThreads()
  {
    lock.lock();
    try
    {
      return addThreads;
    } finally
    {
      lock.unlock();
    }
  }

  public void setAddThreads(int addThreads)
  {
    lock.lock();
    try
    {
      this.addThreads = addThreads;
    } finally
    {
      lock.unlock();
    }
  }

  public long getIdleTimeout()
  {
    lock.lock();
    try
    {
      return idleTimeout;
    } finally
    {
      lock.unlock();
    }
  }

  public void setIdleTimeout(long idleTimeout)
  {
    lock.lock();
    try
    {
      this.idleTimeout = idleTimeout;
    } finally
    {
      lock.unlock();
    }
  }

  public int getNumberRunningThreads()
  {
    lock.lock();
    try
    {
      return runningCount;
    } finally
    {
      lock.unlock();
    }
  }

  public int getNumberIdlingThreads()
  {
    lock.lock();
    try
    {
      return idleCount;
    } finally
    {
      lock.unlock();
    }
  }

  /**
   * @param task
   */
  public void dispatchTask(AsyncTask task)
  {
    lock.lock();
    try
    {
      if (closed || stopped)
        return;
      if (traceSpace.enabled)
        traceSpace.trace(tracePrefix, "dispatchTask, dispatchToken=" + task.getDispatchToken() +
            ", description=" + task.getDescription());
      taskList.add(task);
      if (traceSpace.enabled)
        traceSpace.trace(tracePrefix, "dispatchTask, maxThreads=" + maxThreads + ", size=" + taskList.getSize() + ", idle=" + idleCount + ", running=" + runningCount);
      if (!freezed)
      {
        int running = runningCount;
        int act = idleCount + running;
        if (act < maxThreads || maxThreads == -1)
        {
          if (idleCount == 0 && taskList.getSize() - idleCount >= threshold)
          {
            if (traceSpace.enabled)
              traceSpace.trace(tracePrefix, "dispatchTask, threshold of " + threshold + " reached, starting " + addThreads + " additional threads...");
            for (int i = 0; i < addThreads; i++)
              createNewThread(idleTimeout);
          } else if (act == 0)
          {
            if (traceSpace.enabled)
              traceSpace.trace(tracePrefix, "dispatchTask, no threads running, start up 1 thread...");
            createNewThread(idleTimeout);
          }
        }
        if (idleCount > 0)
        {
          if (traceSpace.enabled) traceSpace.trace(tracePrefix, "dispatchTask, idle=" + idleCount + ", notify...");
          taskAvail.signal();
        }
      }
    } finally
    {
      lock.unlock();
    }
  }

  AsyncTask getNextTask(PoolThread pt, long timeout, AsyncTask oldTask)
  {
    lock.lock();
    try
    {
      if (traceSpace.enabled)
        traceSpace.trace(tracePrefix, "getNextTask, idle=" + idleCount + ", timeout=" + timeout + ", entering...");
      runningCount--;
      if (stopped)
        return null;
      if (maxThreads > 0 && maxThreads <= idleCount + runningCount)
      {
        if (traceSpace.enabled)
          traceSpace.trace(tracePrefix, "getNextTask, maxThreads=" + maxThreads + ", idleCount=" + idleCount + ", runningCount=" + runningCount + ", too much threads, this one dies...");
        if (idleCount > 0)
        {
          if (traceSpace.enabled) traceSpace.trace(tracePrefix, "getNextTask, idle=" + idleCount + ", notify...");
          taskAvail.signal();
        }
        return null;
      }
      if (freezed)
      {
        threadSet.remove(pt);
        if (threadSet.size() == 0 && freezeCompletionListener != null)
        {
          if (traceSpace.enabled)
            traceSpace.trace(tracePrefix, "getNextTask, freezed, calling freezeCompletionListener");
          freezeCompletionListener.freezed(this);
          freezeCompletionListener = null;
        }
        if (traceSpace.enabled) traceSpace.trace(tracePrefix, "getNextTask, freezed, returning null");
        return null;
      }
      AsyncTask task = null;
      if (taskList.getSize() == 0 && !closed)
      {
        idleCount++;
        try
        {
          if (timeout > 0)
          {
            if (traceSpace.enabled)
              traceSpace.trace(tracePrefix, "getNextTask, idle=" + idleCount + ", timeout=" + timeout + ", wait(timeout)");
            long waitStart = System.currentTimeMillis();
            do
            {
              taskAvail.await(timeout, TimeUnit.MILLISECONDS);
            }
            while (!freezed && taskList.getSize() == 0 && !closed && System.currentTimeMillis() - waitStart < timeout);
            if (traceSpace.enabled)
              traceSpace.trace(tracePrefix, "getNextTask, idle=" + idleCount + ", timeout=" + timeout + ", wake up, size=" + taskList.getSize());
          } else
          {
            do
            {
              if (traceSpace.enabled)
                traceSpace.trace(tracePrefix, "getNextTask, idle=" + idleCount + ", timeout=" + timeout + ", wait()");
              taskAvail.awaitUninterruptibly();
              if (traceSpace.enabled)
                traceSpace.trace(tracePrefix, "getNextTask, idle=" + idleCount + ", timeout=" + timeout + ", wake up, size=" + taskList.getSize());
            } while (!freezed && taskList.getSize() == 0 && !closed);
          }
        } catch (InterruptedException ignored)
        {
        }
        idleCount--;
      }
      if (freezed)
      {
        threadSet.remove(pt);
        if (threadSet.size() == 0 && freezeCompletionListener != null)
        {
          if (traceSpace.enabled)
            traceSpace.trace(tracePrefix, "getNextTask, freezed, calling freezeCompletionListener");
          freezeCompletionListener.freezed(this);
          freezeCompletionListener = null;
        }
        if (traceSpace.enabled) traceSpace.trace(tracePrefix, "getNextTask, freezed, returning null");
        return null;
      }
      if (closed || taskList.getSize() == 0)
      {
        if (traceSpace.enabled)
          traceSpace.trace(tracePrefix, "getNextTask, idle=" + idleCount + ", timeout=" + timeout + ", returns null (closed=" + closed + ", size=" + taskList.getSize() + ")");
        threadSet.remove(pt);
        return null;
      }
      task = (AsyncTask) taskList.remove();
      if (traceSpace.enabled)
        traceSpace.trace(tracePrefix, "getNextTask, size=" + taskList.getSize() + ", idle=" + idleCount + ", timeout=" + timeout + ", returns: dispatchToken=" + task.getDispatchToken() +
            ", description=" + task.getDescription());
      runningCount++;
      return task;
    } finally
    {
      lock.unlock();
    }
  }

  public void freeze(FreezeCompletionListener freezeCompletionListener)
  {
    lock.lock();
    try
    {
      if (traceSpace.enabled)
        traceSpace.trace(tracePrefix, "freeze, freezeCompletionListener=" + freezeCompletionListener);
      this.freezeCompletionListener = freezeCompletionListener;
      freezed = true;
      if (threadSet.size() == 0)
        createNewThread(idleTimeout);
      else
        taskAvail.signalAll();
    } finally
    {
      lock.unlock();
    }
  }

  public void unfreeze()
  {
    lock.lock();
    try
    {
      if (traceSpace.enabled) traceSpace.trace(tracePrefix, "unfreeze");
      freezed = false;
      if (minThreads > 0)
      {
        for (int i = 0; i < minThreads; i++)
          createNewThread(-1);
      } else if (taskList.getSize() > 0)
        createNewThread(idleTimeout);
    } finally
    {
      lock.unlock();
    }
  }

  public void stop()
  {
    stopped = true;
  }

  public void close()
  {
    lock.lock();
    try
    {
      if (traceSpace.enabled) traceSpace.trace(tracePrefix, "close, idle=" + idleCount);
      closed = true;
      for (Iterator iter = threadSet.iterator(); iter.hasNext();)
      {
        PoolThread t = (PoolThread) iter.next();
        t.die();
      }
      threadSet.clear();
      taskList.clear();
      taskAvail.signalAll();
    } finally
    {
      lock.unlock();
    }
  }
}

