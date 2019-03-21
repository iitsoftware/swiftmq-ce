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

import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.smqp.v600.*;
import com.swiftmq.mgmt.*;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.tools.collection.ArrayListTool;

import javax.jms.*;
import java.util.ArrayList;

public class BrowserManager
{
  ArrayList queueBrowsers = new ArrayList();
  EntityList browserEntityList = null;
  SessionContext ctx = null;

  public BrowserManager(SessionContext ctx)
  {
    this.ctx = ctx;
    if (ctx.sessionEntity != null)
      browserEntityList = (EntityList) ctx.sessionEntity.getEntity("browser");
  }

  public void createBrowser(CreateBrowserRequest request)
  {
    CreateBrowserReply reply = (CreateBrowserReply) request.createReply();
    QueueImpl queue = request.getQueue();
    String messageSelector = request.getMessageSelector();
    MessageSelector msel = null;
    String queueName = null;
    try
    {
      queueName = queue.getQueueName();
    } catch (JMSException ignored)
    {
    }
    try
    {
      if (messageSelector != null)
      {
        msel = new MessageSelector(messageSelector);
        msel.compile();
      }
      if (!ctx.queueManager.isQueueRunning(queueName))
      {
        ctx.logSwiftlet.logWarning("sys$jms", ctx.tracePrefix + "/" + toString() + ": Invalid destination: " + queueName);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + ": Invalid destination: " + queue);
        reply.setOk(false);
        reply.setException(new InvalidDestinationException("Invalid destination: " + queueName));
      } else
      {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + ": Creating browser with selector: " + msel);
        com.swiftmq.swiftlet.queue.QueueBrowser queueBrowser = ctx.queueManager.createQueueBrowser(queueName, ctx.activeLogin, msel);
        int idx = ArrayListTool.setFirstFreeOrExpand(queueBrowsers, queueBrowser);
        reply.setOk(true);
        reply.setQueueBrowserId(idx);
        if (browserEntityList != null)
        {
          Entity browserEntity = browserEntityList.createEntity();
          browserEntity.setName(queueName + "-" + idx);
          browserEntity.setDynamicObject(queueBrowser);
          browserEntity.createCommands();
          Property prop = browserEntity.getProperty("queue");
          prop.setValue(queueName);
          prop.setReadOnly(true);
          prop = browserEntity.getProperty("selector");
          if (msel != null)
          {
            prop.setValue(msel.getConditionString());
          }
          prop.setReadOnly(true);
          browserEntityList.addEntity(browserEntity);
        }
      }
    } catch (InvalidSelectorException e)
    {
      ctx.logSwiftlet.logWarning("sys$jms", ctx.tracePrefix + "/" + toString() + ": CreateBrowser has invalid Selector: " + e);
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + ": CreateBrowser has invalid Selector: " + e);
      reply.setOk(false);
      reply.setException(e);
    } catch (Exception e)
    {
      ctx.logSwiftlet.logWarning("sys$jms", ctx.tracePrefix + "/" + toString() + ": Exception during createQueueBrowser: " + e);
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + ": Exception during createQueueBrowser: " + e);
      reply.setOk(false);
      reply.setException(e);
    }
    reply.send();
  }

  public void fetchBrowserMessage(FetchBrowserMessageRequest request)
  {
    FetchBrowserMessageReply reply = (FetchBrowserMessageReply) request.createReply();
    int browserId = request.getQueueBrowserId();
    try
    {
      com.swiftmq.swiftlet.queue.QueueBrowser browser = (com.swiftmq.swiftlet.queue.QueueBrowser) queueBrowsers.get(browserId);
      if (request.isResetRequired())
        browser.resetBrowser();
      browser.setLastMessageIndex(request.getLastMessageIndex());
      MessageEntry me = (MessageEntry) browser.getNextMessage();
      reply.setOk(true);
      reply.setMessageEntry(me);
    } catch (Exception e)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + ": get next message failed: " + e.getMessage());
      reply.setOk(false);
      reply.setException(e);
    }
    reply.send();
  }

  public void closeBrowser(CloseBrowserRequest request)
  {
    CloseBrowserReply reply = (CloseBrowserReply) request.createReply();
    try
    {
      int browserId = request.getQueueBrowserId();
      com.swiftmq.swiftlet.queue.QueueBrowser browser = (com.swiftmq.swiftlet.queue.QueueBrowser) queueBrowsers.get(browserId);
      if (!browser.isClosed())
        browser.close();
      queueBrowsers.set(browserId, null);
      reply.setOk(true);
      if (browserEntityList != null)
        browserEntityList.removeDynamicEntity(browser);
    } catch (Exception e)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + ": Exception during close queue browser: " + e);
      reply.setOk(false);
      reply.setException(e);
    }
    reply.send();
  }

  public void close()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + ": closing browsers");
    for (int i = 0; i < queueBrowsers.size(); i++)
    {
      com.swiftmq.swiftlet.queue.QueueBrowser b = (com.swiftmq.swiftlet.queue.QueueBrowser) queueBrowsers.get(i);
      if (b != null && !b.isClosed())
      {
        try
        {
          b.close();
        } catch (Exception ignored)
        {
        }
      }
    }
    queueBrowsers.clear();
  }

  public String toString()
  {
    return "BrowserManager";
  }
}

