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

package com.swiftmq.impl.accounting.standard.factoryimpl;

import com.swiftmq.impl.accounting.standard.SwiftletContext;
import com.swiftmq.jms.MapMessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.accounting.AccountingSink;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.swiftlet.queue.QueueSender;

public class QueueSink implements AccountingSink
{
  SwiftletContext ctx = null;
  String queueName = null;
  QueueSender sender = null;
  QueueImpl dest = null;
  int deliveryMode = 0;

  public QueueSink(SwiftletContext ctx, String queueName, int deliveryMode) throws Exception
  {
    this.ctx = ctx;
    this.queueName = queueName;
    this.deliveryMode = deliveryMode;
    if (this.queueName.indexOf('@') == -1)
      this.queueName = this.queueName + '@' + SwiftletManager.getInstance().getRouterName();
    dest = new QueueImpl(this.queueName);
    sender = ctx.queueManager.createQueueSender(this.queueName, null);
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/created");
  }

  public void add(MapMessageImpl msg) throws Exception
  {
    msg.setJMSDestination(dest);
    msg.setJMSDeliveryMode(deliveryMode);
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/add, msg=" + msg);
    QueuePushTransaction tx = sender.createTransaction();
    tx.putMessage(msg);
    tx.commit();
  }

  public void close()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/close");
    try
    {
      sender.close();
    } catch (QueueException e)
    {
    }
  }

  public String toString()
  {
    return "QueueSink, queue=" + queueName;
  }
}
