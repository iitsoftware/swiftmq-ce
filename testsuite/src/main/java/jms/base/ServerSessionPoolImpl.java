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

import javax.jms.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ServerSessionPoolImpl implements ServerSessionPool
{
  List queue = new ArrayList();
  ExecutorService executorService = Executors.newCachedThreadPool();

  public ExecutorService getExecutorService()
  {
    return executorService;
  }

  public synchronized void addServerSession(ServerSession serverSession)
  {
    queue.add(serverSession);
    if (queue.size() == 1)
      notify();
  }

  public synchronized ServerSession getServerSession() throws JMSException
  {
    if (queue.size() == 0)
    {
      try
      {
        wait();
      } catch (InterruptedException e)
      {
      }
    }
    return (ServerSession) queue.remove(0);
  }
}
