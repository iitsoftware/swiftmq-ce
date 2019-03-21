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

package com.swiftmq.impl.routing.single.smqpr;

import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.swiftlet.queue.QueuePullTransaction;
import com.swiftmq.swiftlet.queue.QueueReceiver;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

public class DeliveryRequest extends Request
{
  public String destinationRouter = null;
  public QueueReceiver receiver = null;
  public QueuePullTransaction readTransaction = null;
  public MessageEntry[] entries = null;
  public int len = 0;
  public DeliveryCallback callback = null;

  public DeliveryRequest(String destinationRouter, QueueReceiver receiver, QueuePullTransaction readTransaction, MessageEntry[] entries, int len, DeliveryCallback callback)
  {
    super(0, false);
    this.destinationRouter = destinationRouter;
    this.receiver = receiver;
    this.readTransaction = readTransaction;
    this.entries = entries;
    this.len = len;
    this.callback = callback;
  }

  public DeliveryRequest()
  {
    this(null, null, null, null, 0, null);
  }

  public int getDumpId()
  {
    return SMQRFactory.DELIVERY_REQ;
  }

  protected Reply createReplyInstance()
  {
    return null;
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQRVisitor) visitor).handleRequest(this);
  }

  public String toString()
  {
    return "[DeliveryRequest receiver=" + receiver + ", readTransaction=" + readTransaction + ", entries=" + entries + ", len=" + len + ", callback=" + callback + "]";
  }
}
