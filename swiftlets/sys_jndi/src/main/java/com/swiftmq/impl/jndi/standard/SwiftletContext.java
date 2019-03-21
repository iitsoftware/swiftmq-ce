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

package com.swiftmq.impl.jndi.standard;

import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.threadpool.*;
import com.swiftmq.swiftlet.queue.QueueManager;
import com.swiftmq.swiftlet.topic.TopicManager;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.trace.*;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;

public class SwiftletContext
{
  public SwiftletContext()
  {
    /*${evaltimer1}*/
  }

  public Configuration config = null;
  public Entity root = null;
  public EntityList usageList = null;
  public ThreadpoolSwiftlet threadpoolSwiftlet = null;
  public ThreadPool myTP = null;
  public QueueManager queueManager = null;
  public TopicManager topicManager = null;
  public TimerSwiftlet timerSwiftlet = null;
  public LogSwiftlet logSwiftlet = null;
  public TraceSwiftlet traceSwiftlet = null;
  public TraceSpace traceSpace = null;
  public JNDISwiftletImpl jndiSwiftlet = null;
}
