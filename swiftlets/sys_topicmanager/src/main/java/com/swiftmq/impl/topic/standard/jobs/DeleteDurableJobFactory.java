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
import com.swiftmq.mgmt.EntityList;

import java.util.*;

public class DeleteDurableJobFactory implements JobFactory
{
  TopicManagerImpl topicManager = null;
  TraceSpace traceSpace = null;
  EntityList activeDurableList = null;
  Map parameters = new HashMap();

  public DeleteDurableJobFactory(TopicManagerImpl topicManager, TraceSpace traceSpace, EntityList activeDurableList)
  {
    this.topicManager = topicManager;
    this.traceSpace = traceSpace;
    this.activeDurableList = activeDurableList;
    JobParameter p = new JobParameter("Client Id Predicate","SQL Like Predicate to match for Client Ids",null,true,null);
    parameters.put(p.getName(),p);
    p = new JobParameter("Durable Name Predicate","SQL Like Predicate to match for Durable Names",null,true,null);
    parameters.put(p.getName(),p);
  }

  public String getName()
  {
    return "Delete Durable";
  }

  public String getDescription()
  {
    return "Delete durable Subscribers";
  }

  public Map getJobParameters()
  {
    return parameters;
  }

  public JobParameter getJobParameter(String s)
  {
    return (JobParameter)parameters.get(s);
  }

  public Job getJobInstance()
  {
    if (traceSpace.enabled) traceSpace.trace(topicManager.getName(),toString()+"/getJobInstance");
    return new DeleteDurableJob(topicManager,traceSpace,activeDurableList);
  }

  public void finished(Job job, JobException e)
  {
    if (traceSpace.enabled) traceSpace.trace(topicManager.getName(),toString()+"/finished, job="+job+", jobException="+e);
  }

  public String toString()
  {
    return "[DeleteDurableJobFactory]";
  }
}
