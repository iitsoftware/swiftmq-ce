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

package com.swiftmq.impl.mgmt.standard;

import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.mgmt.protocol.*;
import com.swiftmq.tools.util.*;
import com.swiftmq.tools.dump.Dumpalizer;
import com.swiftmq.tools.requestreply.*;
import com.swiftmq.jms.*;

import javax.jms.Message;
import java.util.*;

public class Listener extends MessageProcessor
{
  static final String TP_LISTENER = "sys$mgmt.listener";
  SwiftletContext ctx = null;
  ThreadPool myTP = null;
  QueueReceiver receiver = null;
  QueuePullTransaction pullTransaction = null;
  boolean closed = false;
  MessageEntry entry = null;

  public Listener(SwiftletContext ctx) throws Exception
  {
    this.ctx = ctx;
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString()+"/creating ...");
    /*${evaltimer5}*/
    myTP = ctx.threadpoolSwiftlet.getPool(TP_LISTENER);
    receiver = ctx.queueManager.createQueueReceiver(ctx.MGMT_QUEUE, null, null);
    pullTransaction = receiver.createTransaction(false);
    pullTransaction.registerMessageProcessor(this);
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString()+"/creating done");
  }

  public boolean isValid()
  {
    return !closed;
  }

  public void processMessage(MessageEntry entry)
  {
    this.entry = entry;
    myTP.dispatchTask(this);
  }

  public void processException(Exception e)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString()+"/processException: "+e);
  }

  public String getDispatchToken()
  {
    return TP_LISTENER;
  }

  public String getDescription()
  {
    return ctx.mgmtSwiftlet.getName()+"/"+toString();
  }

  public void stop()
  {
  }

  public void run()
  {
    try
    {
      pullTransaction.commit();
    } catch (Exception e)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString()+"/run, exception committing tx: "+e+", exiting");
      return;
    }
    try
    {
      BytesMessageImpl msg = (BytesMessageImpl)entry.getMessage();
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString()+"/run, new message: "+msg);
      QueueImpl queue = (QueueImpl)msg.getJMSReplyTo();
      if (queue != null)
      {
        int len = (int)msg.getBodyLength();
        byte[] buffer = new byte[len];
        msg.readBytes(buffer);
        ctx.dispatchQueue.dispatchClientRequest(msg.getStringProperty(MessageImpl.PROP_USER_ID), queue.getQueueName(),buffer);
      } else
      {
        throw new Exception("Protocol error: Missing replyTo!");
      }
    } catch (Exception e)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString()+"/run, exception during processing: "+e);
      ctx.logSwiftlet.logError(ctx.mgmtSwiftlet.getName(), toString()+"/run, exception during processing: "+e);
    }
    if (closed)
      return;
    try
    {
      pullTransaction = receiver.createTransaction(false);
      pullTransaction.registerMessageProcessor(this);
    } catch (Exception e)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString()+"/run, exception creating new tx: "+e+", exiting");
      return;
    }
  }

  public void close()
  {
    closed = true;
    try {
      receiver.close();
    } catch (Exception ignored){}
  }

  public String toString()
  {
    return "Listener";
  }
}
