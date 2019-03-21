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

package com.swiftmq.impl.amqp.amqp.v01_00_00;

import com.swiftmq.amqp.v100.generated.messaging.delivery_state.DeliveryStateIF;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AddressIF;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AddressString;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AddressVisitor;
import com.swiftmq.impl.amqp.SwiftletContext;
import com.swiftmq.impl.amqp.accounting.AccountingProfile;
import com.swiftmq.impl.amqp.accounting.DestinationCollector;
import com.swiftmq.impl.amqp.accounting.DestinationCollectorCache;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.AuthenticationException;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.topic.TopicException;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.util.SwiftUtilities;

import javax.jms.Destination;
import javax.jms.InvalidSelectorException;
import java.net.MalformedURLException;
import java.util.Set;

public abstract class ServerLink
{
  SwiftletContext ctx = null;
  SessionHandler mySessionHandler = null;
  String name;
  Set offeredCapabilities = null;
  Set desiredCapabilities = null;
  int handle;
  long remoteHandle;
  AddressIF remoteAddress;
  AddressIF localAddress;
  Destination localDestination;
  boolean dynamic = false;
  String exception = null;
  boolean closed = false;
  boolean isQueue = false;
  POObject waitingPO = null;

  public ServerLink(SwiftletContext ctx, SessionHandler mySessionHandler, String name)
  {
    this.ctx = ctx;
    this.mySessionHandler = mySessionHandler;
    this.name = name;
  }

  public String getName()
  {
    return name;
  }

  protected void setHandle(int handle)
  {
    this.handle = handle;
  }

  protected int getHandle()
  {
    return handle;
  }

  protected void setRemoteHandle(long remoteHandle)
  {
    this.remoteHandle = remoteHandle;
  }

  public AddressIF getRemoteAddress()
  {
    return remoteAddress;
  }

  protected void setRemoteAddress(AddressIF remoteAddress)
  {
    this.remoteAddress = remoteAddress;
  }

  protected long getRemoteHandle()
  {
    return remoteHandle;
  }

  public Set getOfferedCapabilities()
  {
    return offeredCapabilities;
  }

  public void setOfferedCapabilities(Set offeredCapabilities)
  {
    this.offeredCapabilities = offeredCapabilities;
  }

  public Set getDesiredCapabilities()
  {
    return desiredCapabilities;
  }

  public void setDesiredCapabilities(Set desiredCapabilities)
  {
    this.desiredCapabilities = desiredCapabilities;
  }

  public boolean isClosed()
  {
    return closed;
  }

  public void setLocalAddress(AddressIF localAddress)
  {
    this.localAddress = localAddress;
  }

  public AddressIF getLocalAddress()
  {
    return localAddress;
  }

  public Destination getLocalDestination()
  {
    return localDestination;
  }

  public boolean isDynamic()
  {
    return dynamic;
  }

  public void setDynamic(boolean dynamic)
  {
    this.dynamic = dynamic;
  }

  public SessionHandler getMySessionHandler()
  {
    return mySessionHandler;
  }

  public POObject getWaitingPO()
  {
    return waitingPO;
  }

  public void setWaitingPO(POObject waitingPO)
  {
    this.waitingPO = waitingPO;
  }

  public abstract void createCollector(AccountingProfile accountingProfile, DestinationCollectorCache cache);

  public abstract void removeCollector();

  public abstract DestinationCollector getCollector();

  public void verifyLocalAddress() throws AuthenticationException, QueueException, TopicException, InvalidSelectorException
  {
    if (localAddress == null)
      throw new QueueException("Local address not set!");
    localAddress.accept(new AddressVisitor()
    {
      public void visit(AddressString addressString)
      {
        try
        {
          String s = SwiftUtilities.extractAMQPName(addressString.getValue());
          if (!ctx.queueManager.isQueueRunning(s))
          {
            if (!ctx.topicManager.isTopicDefined(s))
              exception = "Neither a queue nor a topic is defined with that name: " + addressString.getValue();
            else
            {
              localDestination = new TopicImpl(ctx.topicManager.getQueueForTopic(s), s);
            }
          } else
          {
            if (!ctx.queueManager.isTemporaryQueue(s) && ctx.queueManager.isSystemQueue(s))
              exception = "Tried to access a system queue: " + s;
            else
            {
              if (s.indexOf('@') == -1)
                s += '@' + SwiftletManager.getInstance().getRouterName();
              localDestination = new QueueImpl(s);
              isQueue = true;
            }
          }
        } catch (MalformedURLException e)
        {
          exception = e.toString() + " (" + addressString.getValue() + ")";
        }
      }
    });
    if (exception != null)
      throw new QueueException(exception);
  }

  public abstract void settle(long deliveryId, DeliveryStateIF deliveryState) throws EndWithErrorException;

  public abstract void setUsage(Entity usage);

  public abstract void fillUsage();

  public void close()
  {
    closed = true;
    if (waitingPO != null)
    {
      waitingPO.setSuccess(false);
      waitingPO.setException("ServerLink closed");
      Semaphore sem = waitingPO.getSemaphore();
      if (sem != null)
        sem.notifySingleWaiter();
      waitingPO = null;
    }
  }
}
