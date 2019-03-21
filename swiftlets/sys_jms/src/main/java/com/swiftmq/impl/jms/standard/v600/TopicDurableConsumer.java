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

package com.swiftmq.impl.jms.standard.v600;

import com.swiftmq.jms.TopicImpl;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.topic.TopicManager;

public class TopicDurableConsumer extends Consumer
{
  TopicManager topicManager = null;
  int subscriberId = -1;
  TopicImpl topic = null;
  String queueName = null;

  protected TopicDurableConsumer(SessionContext ctx, String durableName, TopicImpl topic, String selector, boolean noLocal)
    throws Exception
  {
    super(ctx);
    MessageSelector msel = null;
    if (selector != null)
    {
      msel = new MessageSelector(selector);
      msel.compile();
    }
    this.topic = ctx.topicManager.verifyTopic(topic);
    queueName = ctx.topicManager.subscribeDurable(durableName, topic, msel, noLocal, ctx.activeLogin);
    setQueueReceiver(ctx.queueManager.createQueueReceiver(queueName, ctx.activeLogin, null));
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/created");
  }

  public String getQueueName()
  {
    return queueName;
  }

  public String toString()
  {
    return "TopicDurableConsumer, topic=" + topic;
  }
}

