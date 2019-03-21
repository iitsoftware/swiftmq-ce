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

package com.swiftmq.impl.queue.standard.cluster;

public class MessageGroupEntry
{
  private Object key = null;
  private String queueName = null;
  private long lastDispatchTime = 0;

  public MessageGroupEntry(Object key, String queueName)
  {
    this.key = key;
    this.queueName = queueName;
    this.lastDispatchTime = System.currentTimeMillis();
  }

  public Object getKey()
  {
    return key;
  }

  public String getQueueName()
  {
    return queueName;
  }

  public long getLastDispatchTime()
  {
    return lastDispatchTime;
  }

  public void setLastDispatchTime(long lastDispatchTime)
  {
    this.lastDispatchTime = lastDispatchTime;
  }

  public String toString()
  {
    return "[GroupEntry, key=" + key + ", queueName=" + queueName + "]";
  }
}