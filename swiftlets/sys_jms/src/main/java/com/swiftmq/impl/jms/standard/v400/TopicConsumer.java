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

package com.swiftmq.impl.jms.standard.v400;

import com.swiftmq.jms.*;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.topic.TopicManager;

public class TopicConsumer extends Consumer
{
  protected TopicManager topicManager = null;
  protected TopicImpl topic = null;
  protected int subscriberId = -1;
  protected String queueName = null;

  protected TopicConsumer(SessionContext ctx, TopicImpl topic, String selector, boolean noLocal)
    throws Exception
  {
    super(ctx);
    this.topic = topic;
    MessageSelector msel = null;
    if (selector != null)
    {
      msel = new MessageSelector(selector);
      msel.compile();
    }
    if (topic.getType() == DestinationFactory.TYPE_TOPIC)
    {
      this.topic = ctx.topicManager.verifyTopic(topic);
      queueName = ctx.queueManager.createTemporaryQueue();
    } else
      queueName = topic.getQueueName();
    try
    {
      if (topic.getType() == DestinationFactory.TYPE_TOPIC)
      {
        subscriberId = ctx.topicManager.subscribe(topic, msel, noLocal, queueName, ctx.activeLogin);
        setQueueReceiver(ctx.queueManager.createQueueReceiver(queueName, ctx.activeLogin, null));
      } else
        setQueueReceiver(ctx.queueManager.createQueueReceiver(queueName, ctx.activeLogin, msel));
      setSelector(msel);
    } catch (Exception e)
    {
      if (topic.getType() == DestinationFactory.TYPE_TOPIC)
        ctx.queueManager.deleteTemporaryQueue(queueName);
      throw e;
    }
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/created");
  }

  public String getQueueName()
  {
    return queueName;
  }

  protected boolean isAutoCommit()
  {
    return !ctx.transacted && ctx.ackMode != javax.jms.Session.CLIENT_ACKNOWLEDGE;
  }

  protected void close() throws Exception
  {
    super.close();
    if (topic.getType() == DestinationFactory.TYPE_TOPIC)
    {
      ctx.topicManager.unsubscribe(subscriberId);
      ctx.queueManager.deleteTemporaryQueue(queueName);
    }
  }

  public String toString()
  {
    return "TopicConsumer, topic=" + topic;
  }
}

