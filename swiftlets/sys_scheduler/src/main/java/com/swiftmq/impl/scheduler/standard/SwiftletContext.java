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

package com.swiftmq.impl.scheduler.standard;

import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.queue.QueueManager;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.topic.TopicManager;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;

public class SwiftletContext
{
  public static final String JOBGROUP = "Scheduler";
  public static final String JOBNAME = "Message Sender";
  public static final String PARM = "ID";
  public static final String PROP_SCHED_ID = "JMS_SWIFTMQ_SCHEDULER_ID";
  public static final String PROP_SCHED_DEST = "JMS_SWIFTMQ_SCHEDULER_DESTINATION";
  public static final String PROP_SCHED_DEST_TYPE = "JMS_SWIFTMQ_SCHEDULER_DESTINATION_TYPE";
  public static final String PROP_SCHED_CALENDAR = "JMS_SWIFTMQ_SCHEDULER_CALENDAR";
  public static final String PROP_SCHED_TIME = "JMS_SWIFTMQ_SCHEDULER_TIME_EXPRESSION";
  public static final String PROP_SCHED_EXPIRATION = "JMS_SWIFTMQ_SCHEDULER_EXPIRATION";
  public static final String PROP_SCHED_FROM = "JMS_SWIFTMQ_SCHEDULER_DATE_FROM";
  public static final String PROP_SCHED_TO = "JMS_SWIFTMQ_SCHEDULER_DATE_TO";
  public static final String PROP_SCHED_ENABLE_LOGGING = "JMS_SWIFTMQ_SCHEDULER_ENABLE_LOGGING";
  public static final String PROP_SCHED_MAY_EXPIRE = "JMS_SWIFTMQ_SCHEDULER_MAY_EXPIRE_WHILE_ROUTER_DOWN";
  public static final String REQUEST_QUEUE = "swiftmqscheduler";
  public static final String INTERNAL_QUEUE = "sys$scheduler";
  public static final String QUEUE = "queue";
  public static final String TOPIC = "topic";
  public SchedulerSwiftletImpl schedulerSwiftlet = null;
  public TraceSwiftlet traceSwiftlet = null;
  public TraceSpace traceSpace = null;
  public LogSwiftlet logSwiftlet = null;
  public TimerSwiftlet timerSwiftlet = null;
  public ThreadpoolSwiftlet threadpoolSwiftlet = null;
  public QueueManager queueManager = null;
  public TopicManager topicManager = null;
  public Entity root = null;
  public EntityList jobGroupList = null;
  public EntityList activeScheduleList = null;
  public EntityList activeMessageScheduleList = null;
  public Scheduler scheduler = null;

  public SwiftletContext(SchedulerSwiftletImpl schedulerSwiftlet, Entity root) throws Exception
  {
    this.schedulerSwiftlet = schedulerSwiftlet;
    this.root = root;
    traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
    logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
    timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");
    threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
    queueManager = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
    topicManager = (TopicManager) SwiftletManager.getInstance().getSwiftlet("sys$topicmanager");
    traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
    jobGroupList = (EntityList)root.getEntity("usage").getEntity("job-groups");
    activeScheduleList = (EntityList)root.getEntity("usage").getEntity("active-job-schedules");
    activeMessageScheduleList = (EntityList)root.getEntity("usage").getEntity("active-message-schedules");
  }

  public void close() throws Exception
  {
  }
}
