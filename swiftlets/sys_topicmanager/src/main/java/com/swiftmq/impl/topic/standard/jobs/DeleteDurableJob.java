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

package com.swiftmq.impl.topic.standard.jobs;

import com.swiftmq.swiftlet.scheduler.*;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.impl.topic.standard.TopicManagerImpl;
import com.swiftmq.mgmt.*;
import com.swiftmq.tools.sql.LikeComparator;

import java.util.*;

public class DeleteDurableJob implements Job
{
  TopicManagerImpl topicManager = null;
  TraceSpace traceSpace = null;
  EntityList activeDurableList = null;
  boolean stopCalled = false;
  Properties properties = null;

  public DeleteDurableJob(TopicManagerImpl topicManager, TraceSpace traceSpace, EntityList activeDurableList)
  {
    this.topicManager = topicManager;
    this.traceSpace = traceSpace;
    this.activeDurableList = activeDurableList;
  }

  public void start(Properties properties, JobTerminationListener jobTerminationListener) throws JobException
  {
    if (traceSpace.enabled) traceSpace.trace(topicManager.getName(), toString() + "/start, properties=" + properties + " ...");
    this.properties = properties;
    if (stopCalled)
      return;
    Map entities = activeDurableList.getEntities();
    int cnt = 0;
    if (entities != null)
    {
      String cPred = properties.getProperty("Client Id Predicate");
      String dPred = properties.getProperty("Durable Name Predicate");
      for (Iterator iter=entities.entrySet().iterator();iter.hasNext();)
      {
        Entity durable = (Entity)((Map.Entry)iter.next()).getValue();
        String clientId = (String)durable.getProperty("clientid").getValue();
        String durableName = (String)durable.getProperty("durablename").getValue();
        if (LikeComparator.compare(clientId, cPred, '\\') && LikeComparator.compare(durableName, dPred, '\\'))
        {
          try
          {
            activeDurableList.removeEntity(durable);
            cnt++;
          } catch (EntityRemoveException e)
          {
          }
        }
        if (stopCalled)
          return;
      }
    }
    jobTerminationListener.jobTerminated(cnt+" durable Subscribers deleted");
  }

  public void stop() throws JobException
  {
    if (traceSpace.enabled) traceSpace.trace(topicManager.getName(), toString() + "/stop ...");
    stopCalled = true;
    if (traceSpace.enabled) traceSpace.trace(topicManager.getName(), toString() + "/stop done");
  }

  public String toString()
  {
    return "[DeleteDurableJob, properties=" + properties + "]";
  }
}
