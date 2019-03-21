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

package com.swiftmq.impl.amqp.amqp.v00_09_01;

import com.swiftmq.amqp.v091.generated.Constants;
import com.swiftmq.amqp.v091.generated.basic.*;
import com.swiftmq.amqp.v091.generated.channel.*;
import com.swiftmq.amqp.v091.generated.exchange.*;
import com.swiftmq.amqp.v091.generated.queue.Purge;
import com.swiftmq.amqp.v091.generated.queue.PurgeOk;
import com.swiftmq.amqp.v091.generated.queue.QueueMethod;
import com.swiftmq.amqp.v091.generated.queue.QueueMethodVisitor;
import com.swiftmq.amqp.v091.generated.tx.*;
import com.swiftmq.amqp.v091.types.ContentHeaderProperties;
import com.swiftmq.amqp.v091.types.Frame;
import com.swiftmq.amqp.v091.types.Method;
import com.swiftmq.impl.amqp.SwiftletContext;
import com.swiftmq.impl.amqp.VersionedConnection;
import com.swiftmq.impl.amqp.amqp.v00_09_01.po.*;
import com.swiftmq.impl.amqp.amqp.v00_09_01.transformer.*;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.TemporaryQueueImpl;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.AuthenticationException;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.PipelineQueue;
import com.swiftmq.util.SwiftUtilities;

import javax.jms.Destination;
import javax.jms.JMSException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ChannelHandler implements AMQPChannelVisitor, DestinationFactory
{
  static final POActivateFlow PO_ACTIVATE_FLOW = new POActivateFlow();
  SwiftletContext ctx;
  AMQPHandler amqpHandler;
  int channelNo = 0;
  VersionedConnection versionedConnection = null;
  PipelineQueue pipelineQueue = null;
  boolean closed = false;
  boolean closeInProgress = false;
  boolean channelDisabled = false;
  Lock closeLock = new ReentrantLock();
  ChannelMethodVisitor channelMethodVisitor = new ChannelMethodVisitorImpl();
  ExchangeMethodVisitor exchangeMethodVisitor = new ExchangeMethodVisitorImpl();
  QueueMethodVisitor queueMethodVisitor = new QueueMethodVisitorImpl();
  BasicMethodVisitor basicMethodVisitor = new BasicMethodVisitorImpl();
  TxMethodVisitor txMethodVisitor = new TxMethodVisitorImpl();
  MessageWrap currentMessage = null;
  Map bindings = new HashMap();
  Map producers = new HashMap();
  Map consumers = new HashMap();
  InboundTransformer inboundTransformer = new BasicInboundTransformer();
  long prefetchSizeLimit = 0;
  int prefetchCountLimit = 0;
  long unackedPrefetchedSize = 0;
  int unackedPrefetchedCount = 0;
  boolean flowActive = true;
  boolean remoteFlowActive = true;
  int consumerCount = 0;
  List unacked = new ArrayList();
  long deliveryCnt = 0;
  List<POSendMessages> waitingSends = new ArrayList<POSendMessages>();
  OutboundTransformer outboundTransformer = new BasicOutboundTransformer();

  public ChannelHandler(SwiftletContext ctx, AMQPHandler amqpHandler, int channelNo)
  {
    this.ctx = ctx;
    this.amqpHandler = amqpHandler;
    this.channelNo = channelNo;
    versionedConnection = amqpHandler.getVersionedConnection();
    pipelineQueue = new PipelineQueue(ctx.threadpoolSwiftlet.getPool(VersionedConnection.TP_SESSIONSVC), "ChannelHandler", this);
  }

  public int getChannelNo()
  {
    return channelNo;
  }

  public VersionedConnection getVersionedConnection()
  {
    return versionedConnection;
  }

  public void dispatch(POObject po)
  {
    pipelineQueue.enqueue(po);
  }

  private void handleMethodFrame(Frame frame)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", handleMethodFrame");
    try
    {
      Method method = (Method) frame.getPayloadObject();
      switch (method._getClassId())
      {
        case 20:
          ((ChannelMethod) method).accept(channelMethodVisitor);
          break;
        case 40:
          ((ExchangeMethod) method).accept(exchangeMethodVisitor);
          break;
        case 50:
          ((QueueMethod) method).accept(queueMethodVisitor);
          break;
        case 60:
          ((BasicMethod) method).accept(basicMethodVisitor);
          break;
        case 90:
          ((TxMethod) method).accept(txMethodVisitor);
          break;
        default:
          throw new Exception("Invalid class id: " + method._getClassId());
      }
    } catch (Exception e)
    {
      amqpHandler.dispatch(new POSendClose(Constants.FRAME_ERROR, e.getMessage() == null ? e.toString() : e.getMessage()));
    }
  }

  private String getMapping(String name)
  {
    String s = ctx.queueMapper.get(name);
    return s != null ? s : name;
  }

  public Destination create(String name)
  {
    Destination dest = null;
    if (ctx.queueManager.isQueueRunning(name))
    {
      if (name.indexOf('@') == -1)
        name += "@" + SwiftletManager.getInstance().getRouterName();
      if (ctx.queueManager.isTemporaryQueue(name))
        dest = new TemporaryQueueImpl(name, null);
      else
        dest = new QueueImpl(name);
    } else
    {
      if (ctx.topicManager.isTopicDefined(name))
        dest = new TopicImpl(ctx.topicManager.getQueueForTopic(name), name);
      else
        dest = null;
    }
    return dest;
  }

  private boolean isWithinPrefetchLimits(int msgSize)
  {
    boolean rc = flowActive && (prefetchCountLimit == 0 || unackedPrefetchedCount <= prefetchCountLimit) && (prefetchSizeLimit == 0 || unackedPrefetchedSize + msgSize <= prefetchSizeLimit);
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", isWithinPrefetchLimits, flowActive=" + flowActive + ", msgSize=" + msgSize + ", prefetchCountLimit=" + prefetchCountLimit + ", unackedPrefetchedCount=" + unackedPrefetchedCount + ", prefetchSizeLimit=" + prefetchSizeLimit + ", unackedPrefetchedSize=" + unackedPrefetchedSize + " returns " + rc);
    if (rc)
    {
      if (prefetchCountLimit > 0)
        unackedPrefetchedCount++;
      if (prefetchSizeLimit > 0)
        unackedPrefetchedSize += msgSize;
    }
    return rc;
  }

  private void dispatchWaitingSends()
  {
    if (waitingSends.size() > 0)
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", dispatch waiting sends");
      POSendMessages[] po = waitingSends.toArray(new POSendMessages[waitingSends.size()]);
      waitingSends.clear();
      for (int i = 0; i < po.length; i++)
        ChannelHandler.this.visit(po[i]);
    }
  }

  private void publishCurrentMessage(Exchange exchange)
  {
    if (exchange.getType() == Exchange.DIRECT)
    {
      String rk = getMapping(currentMessage.getPublish().getRoutingKey());
      QueueSender sender = (QueueSender) producers.get(rk);
      try
      {
        if (sender == null)
        {
          if (producers.size() > 10)
          {
            if (ctx.traceSpace.enabled)
              ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", sender cache > 10, closing all");
            for (Iterator iter = producers.entrySet().iterator(); iter.hasNext(); )
            {
              QueueSender s = (QueueSender) ((Map.Entry) iter.next()).getValue();
              if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", closing sender for queue: " + s.getQueueName());
              s.close();
              iter.remove();
            }
          }
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", creating new sender on queue: " + rk);
          sender = ctx.queueManager.createQueueSender(rk, versionedConnection.getActiveLogin());
          producers.put(rk, sender);

        }
        QueuePushTransaction t = sender.createTransaction();
        MessageImpl msg = inboundTransformer.transform(currentMessage, this);
        msg.setJMSDestination(create(rk));
        msg.reset();
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", converted JMS message: " + msg);
        t.putMessage(msg);
        t.commit();
        if (remoteFlowActive)
        {
          long fcDelay = t.getFlowControlDelay();
          if (fcDelay > 100)
          {
            sendFlow(false);
            ctx.timerSwiftlet.addInstantTimerListener(fcDelay, new TimerListener()
            {
              public void performTimeAction()
              {
                dispatch(PO_ACTIVATE_FLOW);
              }
            });
          }
        }
      } catch (JMSException e)
      {
        amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Exception on publishing to exchange '" + currentMessage.getPublish().getExchange() + "': " + e, currentMessage.getPublish()));
      }
    } else
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.NOT_IMPLEMENTED, "Exchange is not of type direct: " + currentMessage.getPublish().getExchange(), currentMessage.getPublish()));
  }

  private void sendFlow(boolean activate)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", sendFlow (" + activate + ")");
    remoteFlowActive = activate;
    Flow flow = new Flow();
    flow.setActive(activate);
    Frame frame = new Frame(Frame.TYPE_METHOD, channelNo, 0, null);
    amqpHandler.toPayload(frame, flow);
    versionedConnection.send(frame);
  }

  private void publishCurrentMessage() throws Exception
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", publishCurrentMessage ...");

    try
    {
      Exchange exchange = ctx.exchangeRegistry.get(currentMessage.getPublish().getExchange());
      if (exchange != null)
        publishCurrentMessage(exchange);
      else
        amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.NOT_FOUND, "Unknown exchange: " + currentMessage.getPublish().getExchange(), currentMessage.getPublish()));
      currentMessage = null;
    } catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }

    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", publishCurrentMessage done");
  }

  private void handleHeaderFrame(Frame frame)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", handleHeaderFrame");
    if (currentMessage != null && currentMessage.getContentHeaderProperties() == null)
    {
      try
      {
        ContentHeaderProperties contentHeaderProperties = (ContentHeaderProperties) frame.getPayloadObject();
        currentMessage.setContentHeaderProperties(contentHeaderProperties);
        if (contentHeaderProperties.getBodySize() == 0)
          publishCurrentMessage();
      } catch (Exception e)
      {
        amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, e.getMessage() == null ? e.toString() : e.getMessage(), currentMessage.getPublish()));
      }
    } else
      amqpHandler.dispatch(new POSendClose(Constants.UNEXPECTED_FRAME, "Unexpected: " + frame));
  }

  private void handleBodyFrame(Frame frame)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", handleBodyFrame+");
    if (currentMessage != null && currentMessage.getContentHeaderProperties() != null)
    {
      try
      {
        if (currentMessage.addBodyPart(frame.getPayload()))
          publishCurrentMessage();
      } catch (Exception e)
      {
        amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, e.getMessage() == null ? e.toString() : e.getMessage(), currentMessage.getPublish()));
      }
    } else
      amqpHandler.dispatch(new POSendClose(Constants.UNEXPECTED_FRAME, "Unexpected: " + frame));
  }

  public long getNextDeliveryTag()
  {
    if (deliveryCnt == Long.MAX_VALUE)
      deliveryCnt = 0;
    return deliveryCnt++;
  }

  public void addUnacked(long deliveryTag, Consumer consumer, MessageIndex messageIndex, int size)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/addUnacked, deliveryTag=" + deliveryTag + ", size=" + size + ", consumer=" + consumer + ", messageIndex=" + messageIndex);
    unacked.add(new UnackedEntry(deliveryTag, consumer, messageIndex, size));
  }

  public void visit(POChannelFrameReceived po)
  {
    if (channelDisabled)
      return;
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
    Frame frame = po.getFrame();
    switch (frame.getType())
    {
      case Frame.TYPE_METHOD:
        handleMethodFrame(frame);
        break;
      case Frame.TYPE_HEADER:
        handleHeaderFrame(frame);
        break;
      case Frame.TYPE_BODY:
        handleBodyFrame(frame);
        break;
      default:
        amqpHandler.dispatch(new POSendClose(Constants.FRAME_ERROR, "Invalid frame type: " + frame.getType()));
        break;
    }
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
  }

  public void visit(POSendMessages po)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
    try
    {
      SourceMessageProcessor mp = po.getSourceMessageProcessor();
      Consumer consumer = mp.getConsumer();
      if (mp.getException() != null)
        amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, mp.getException().toString(), null));
      else
      {
        if (!consumer.isNoAck() && waitingSends.size() > 0)
        {
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " sends waiting, adding to list");
          waitingSends.add(po);
          return;
        }
        Delivery[] deliveries = po.getDeliveries() != null ? po.getDeliveries() : mp.getTransformedMessages();
        for (int i = po.getDeliveryStart(); i < deliveries.length; i++)
        {
          Delivery delivery = deliveries[i];
          if (!consumer.isNoAck() && !isWithinPrefetchLimits(delivery.getBody().length))
          {
            if (ctx.traceSpace.enabled)
              ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " not within prefetch limit, adding to list");
            po.setDeliveryStart(i);
            po.setDeliveries(deliveries);
            waitingSends.add(po);
            return;
          }
          long deliveryTag = getNextDeliveryTag();
          if (!consumer.isNoAck())
            addUnacked(deliveryTag, consumer, delivery.getMessageIndex(), delivery.getBody().length);
          else
            consumer.ack(delivery.getMessageIndex());
          Deliver deliver = new Deliver();
          deliver.setConsumerTag(mp.getConsumer().getConsumerTag());
          deliver.setDeliveryTag(deliveryTag);
          deliver.setExchange("");
          deliver.setRedelivered(delivery.isRedelivered());
          deliver.setRoutingKey(mp.getConsumer().getOriginalQueueName());
          Frame frame = new Frame(Frame.TYPE_METHOD, channelNo, 0, null);
          amqpHandler.toPayload(frame, deliver);
          versionedConnection.send(frame);
          frame = new Frame(Frame.TYPE_HEADER, channelNo, 0, null);
          ContentHeaderProperties contentHeaderProperties = delivery.getContentHeaderProperties();
          contentHeaderProperties.setClassId(deliver._getClassId());
          amqpHandler.toPayload(frame, contentHeaderProperties);
          versionedConnection.send(frame);
          byte[] body = delivery.getBody();
          int maxFrameSize = amqpHandler.getMaxFrameSize();
          int pos = 0;
          while (pos < body.length)
          {
            byte[] part = new byte[Math.min(body.length - pos, maxFrameSize)];
            System.arraycopy(body, pos, part, 0, part.length);
            pos += part.length;
            frame = new Frame(Frame.TYPE_BODY, channelNo, 0, null);
            frame.setPayload(part);
            frame.setSize(part.length);
            versionedConnection.send(frame);
          }
        }
        mp.getConsumer().startMessageProcessor(mp);
      }
    } catch (Exception e)
    {
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, e.toString(), null));
    }
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
  }

  public void visit(POActivateFlow po)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
    sendFlow(true);
  }

  public void visit(POCloseChannel po)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
    for (int i = 0; i < unacked.size(); i++)
    {
      UnackedEntry unackedEntry = (UnackedEntry) unacked.get(i);
      try
      {
        if (unackedEntry.isGet)
          unackedEntry.unack();
        else
          unackedEntry.getConsumer().reject(unackedEntry.getMessageIndex());
      } catch (Exception e)
      {
      }
    }
    unacked.clear();
    for (Iterator iter = producers.entrySet().iterator(); iter.hasNext(); )
    {
      QueueSender s = (QueueSender) ((Map.Entry) iter.next()).getValue();
      try
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", closing sender for queue: " + s.getQueueName());
        s.close();
      } catch (QueueException e)
      {
      }
      iter.remove();
    }
    for (Iterator iter = consumers.entrySet().iterator(); iter.hasNext(); )
    {
      Consumer s = (Consumer) ((Map.Entry) iter.next()).getValue();
      try
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", closing consumer: " + s);
        s.close();
      } catch (Exception e)
      {
      }
      iter.remove();
    }
    closed = true;
    pipelineQueue.close();
    po.setSuccess(true);
    if (po.getSemaphore() != null)
      po.getSemaphore().notifySingleWaiter();
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
  }

  public void close()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", close ...");
    closeLock.lock();
    if (closeInProgress)
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", close in progress, return");
      return;
    }
    closeInProgress = true;
    closeLock.unlock();
    channelDisabled = false;
    Semaphore sem = new Semaphore();
    dispatch(new POCloseChannel(sem));
    sem.waitHere();
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", close done");
  }

  public String toString()
  {
    return amqpHandler.toString() + "/ChannelHandler, channelNo=" + channelNo;
  }

  private class ChannelMethodVisitorImpl implements ChannelMethodVisitor
  {
    public void visit(Open method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public void visit(OpenOk method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public void visit(Flow method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      flowActive = method.getActive();
      FlowOk flowOk = new FlowOk();
      flowOk.setActive(flowActive);
      Frame frame = new Frame(Frame.TYPE_METHOD, channelNo, 0, null);
      amqpHandler.toPayload(frame, flowOk);
      versionedConnection.send(frame);
      if (flowActive)
        dispatchWaitingSends();
    }

    public void visit(FlowOk method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      remoteFlowActive = method.getActive();
    }

    public void visit(Close method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public void visit(CloseOk method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public String toString()
    {
      return ChannelHandler.this.toString() + "/ChannelMethodVisitorImpl";
    }
  }

  private class ExchangeMethodVisitorImpl implements ExchangeMethodVisitor
  {
    public void visit(final Declare method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      if (method.getPassive())
      {
        if (ctx.exchangeRegistry.get(method.getExchange()) != null)
        {
          DeclareOk declareOk = new DeclareOk();
          Frame frame = new Frame(Frame.TYPE_METHOD, channelNo, 0, null);
          amqpHandler.toPayload(frame, declareOk);
          versionedConnection.send(frame);
        } else
          amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.NOT_FOUND, "Exchange not found: " + method.getExchange(), method));
      } else
      {
        try
        {
          ctx.exchangeRegistry.declare(method.getExchange(), new ExchangeFactory()
          {
            public Exchange create() throws Exception
            {
              Exchange r = null;
              if (method.getType().equals("direct"))
              {
                r = new Exchange()
                {
                  public int getType()
                  {
                    return DIRECT;
                  }
                };
              } else if (method.getType().equals("fanout"))
              {
                SwiftUtilities.verifyTopicName(method.getExchange());
                r = new Exchange()
                {
                  public int getType()
                  {
                    return FANOUT;
                  }
                };
              } else if (method.getType().equals("topic"))
              {
                r = new Exchange()
                {
                  public int getType()
                  {
                    return TOPIC;
                  }
                };
              }
              return r;
            }
          });
          if (method.getNoWait())
            return;
          DeclareOk declareOk = new DeclareOk();
          Frame frame = new Frame(Frame.TYPE_METHOD, channelNo, 0, null);
          amqpHandler.toPayload(frame, declareOk);
          versionedConnection.send(frame);
        } catch (Exception e)
        {
          amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.SYNTAX_ERROR, e.getMessage(), method));
        }
      }
    }

    public void visit(DeclareOk method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public void visit(Delete method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      try
      {
        ctx.exchangeRegistry.delete(method.getExchange(), method.getIfUnused());
        if (method.getNoWait())
          return;
        DeleteOk deleteOk = new DeleteOk();
        Frame frame = new Frame(Frame.TYPE_METHOD, channelNo, 0, null);
        amqpHandler.toPayload(frame, deleteOk);
        versionedConnection.send(frame);
      } catch (Exception e)
      {
        amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, e.getMessage(), method));
      }
    }

    public void visit(DeleteOk method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public void visit(Bind method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public void visit(BindOk method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public void visit(Unbind method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public void visit(UnbindOk method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public String toString()
    {
      return ChannelHandler.this.toString() + "/ExchangeMethodVisitorImpl";
    }
  }

  private class QueueMethodVisitorImpl implements QueueMethodVisitor
  {
    private void checkQueue(com.swiftmq.amqp.v091.generated.queue.Declare method)
    {
      String queueName = method.getQueue();
      if (!method.getDurable())
      {
        String s = ctx.queueMapper.get(method.getQueue());
        if (s != null)
          queueName = s;
      }
      try
      {
        AbstractQueue abstractQueue = ctx.queueManager.getQueueForInternalUse(queueName);
        if (abstractQueue != null)
        {
          if (method.getNoWait())
            return;
          com.swiftmq.amqp.v091.generated.queue.DeclareOk declareOk = new com.swiftmq.amqp.v091.generated.queue.DeclareOk();
          declareOk.setQueue(method.getQueue());
          declareOk.setMessageCount((int) abstractQueue.getNumberQueueMessages());
          declareOk.setConsumerCount(abstractQueue.getReceiverCount());
          Frame frame = new Frame(Frame.TYPE_METHOD, channelNo, 0, null);
          amqpHandler.toPayload(frame, declareOk);
          versionedConnection.send(frame);
        } else
          amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.NOT_FOUND, "Queue not found: " + method.getQueue(), method));
      } catch (QueueException e)
      {
        amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, e.toString(), method));
      }
    }

    public void visit(com.swiftmq.amqp.v091.generated.queue.Declare method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      if (method.getPassive())
        checkQueue(method);
      else
      {
        if (method.getDurable())
          checkQueue(method);
        else
        {
          try
          {
            com.swiftmq.amqp.v091.generated.queue.DeclareOk declareOk = new com.swiftmq.amqp.v091.generated.queue.DeclareOk();
            if (method.getQueue().length() > 0)
            {
              AbstractQueue abstractQueue = ctx.queueManager.getQueueForInternalUse(method.getQueue());
              if (abstractQueue == null)
              {
                String name = ctx.queueManager.createTemporaryQueue();
                amqpHandler.addTempQueue(name);
                ctx.queueMapper.map(method.getQueue(), name);
              }
              declareOk.setQueue(method.getQueue());
            } else
            {
              String name = ctx.queueManager.createTemporaryQueue();
              amqpHandler.addTempQueue(name);
              declareOk.setQueue(name);
            }
            if (method.getNoWait())
              return;
            Frame frame = new Frame(Frame.TYPE_METHOD, channelNo, 0, null);
            amqpHandler.toPayload(frame, declareOk);
            versionedConnection.send(frame);
          } catch (QueueException e)
          {
            amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, e.toString(), method));
          }
        }
      }
    }

    public void visit(com.swiftmq.amqp.v091.generated.queue.DeclareOk method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public void visit(com.swiftmq.amqp.v091.generated.queue.Bind method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      Exchange exchange = ctx.exchangeRegistry.get(method.getExchange());
      if (exchange != null)
      {
        String queueName = getMapping(method.getQueue());
        if (ctx.queueManager.isQueueRunning(queueName))
        {
          // Ignore for DIRECT
          if (exchange.getType() != Exchange.DIRECT)
          {
            // Do something
          }
          if (method.getNoWait())
            return;
          com.swiftmq.amqp.v091.generated.queue.BindOk bindOk = new com.swiftmq.amqp.v091.generated.queue.BindOk();
          Frame frame = new Frame(Frame.TYPE_METHOD, channelNo, 0, null);
          amqpHandler.toPayload(frame, bindOk);
          versionedConnection.send(frame);
        } else
          amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.NOT_FOUND, "Queue is unknown: " + method.getQueue(), method));
      } else
        amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.NOT_FOUND, "Exchange is unknown: " + method.getExchange(), method));
    }

    public void visit(com.swiftmq.amqp.v091.generated.queue.BindOk method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public void visit(com.swiftmq.amqp.v091.generated.queue.Unbind method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      Exchange exchange = ctx.exchangeRegistry.get(method.getExchange());
      if (exchange != null)
      {
        String queueName = getMapping(method.getQueue());
        if (ctx.queueManager.isQueueRunning(queueName))
        {
          // Ignore for DIRECT
          if (exchange.getType() != Exchange.DIRECT)
          {
            // Do something
          }
          com.swiftmq.amqp.v091.generated.queue.UnbindOk unbindOk = new com.swiftmq.amqp.v091.generated.queue.UnbindOk();
          Frame frame = new Frame(Frame.TYPE_METHOD, channelNo, 0, null);
          amqpHandler.toPayload(frame, unbindOk);
          versionedConnection.send(frame);
        } else
          amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.NOT_FOUND, "Queue is unknown: " + method.getQueue(), method));
      } else
        amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.NOT_FOUND, "Exchange is unknown: " + method.getExchange(), method));
    }

    public void visit(com.swiftmq.amqp.v091.generated.queue.UnbindOk method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public void visit(Purge method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      QueueReceiver receiver = null;
      try
      {
        String queueName = getMapping(method.getQueue());
        receiver = ctx.queueManager.createQueueReceiver(queueName, amqpHandler.getActiveLogin(), null);
        QueuePullTransaction tx = receiver.createTransaction(false);
        int cnt = 0;
        while (tx.getMessage(0, null) != null)
        {
          tx.commit();
          tx = receiver.createTransaction(false);
          cnt++;
        }
        tx.commit();
        receiver.close();
        if (method.getNoWait())
          return;
        PurgeOk purgeOk = new PurgeOk();
        purgeOk.setMessageCount(cnt);
        Frame frame = new Frame(Frame.TYPE_METHOD, channelNo, 0, null);
        amqpHandler.toPayload(frame, purgeOk);
        versionedConnection.send(frame);
      } catch (QueueException e)
      {
        amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Exception during queue purge: " + e, method));
      } catch (AuthenticationException e)
      {
        amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.NOT_ALLOWED, e.toString(), method));
      } finally
      {
        try
        {
          if (receiver != null)
            receiver.close();
        } catch (QueueException e)
        {
        }
      }
    }

    public void visit(PurgeOk method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public void visit(com.swiftmq.amqp.v091.generated.queue.Delete method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      String queueName = getMapping(method.getQueue());
      if (ctx.queueManager.isTemporaryQueue(queueName))
      {
        amqpHandler.removeTempQueue(queueName);
        try
        {
          ctx.queueManager.deleteTemporaryQueue(queueName);
        } catch (QueueException e)
        {
        }
        ctx.queueMapper.unmap(method.getQueue());
      }
      if (method.getNoWait())
        return;
      com.swiftmq.amqp.v091.generated.queue.DeleteOk deleteOk = new com.swiftmq.amqp.v091.generated.queue.DeleteOk();
      Frame frame = new Frame(Frame.TYPE_METHOD, channelNo, 0, null);
      amqpHandler.toPayload(frame, deleteOk);
      versionedConnection.send(frame);
    }

    public void visit(com.swiftmq.amqp.v091.generated.queue.DeleteOk method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public String toString()
    {
      return ChannelHandler.this.toString() + "/QueueMethodVisitorImpl";
    }
  }

  private class BasicMethodVisitorImpl implements BasicMethodVisitor
  {
    public void visit(Qos method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      if (method.getGlobal())
        amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.NOT_IMPLEMENTED, "Global prefetch settings are not implemented: " + method, method));
      else
      {
        prefetchSizeLimit = method.getPrefetchSize();
        prefetchCountLimit = method.getPrefetchCount();
        QosOk qosOk = new QosOk();
        Frame frame = new Frame(Frame.TYPE_METHOD, channelNo, 0, null);
        amqpHandler.toPayload(frame, qosOk);
        versionedConnection.send(frame);
      }
    }

    public void visit(QosOk method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public void visit(Consume method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      String consumerTag = method.getConsumerTag();
      if (consumerTag.length() == 0)
        consumerTag = "consumer-" + (consumerCount++);
      try
      {
        if (consumers.get(consumerTag) == null)
        {
          Consumer consumer = new Consumer(ctx, ChannelHandler.this, consumerTag, method.getQueue(), getMapping(method.getQueue()), method.getNoAck(), method.getNoLocal());
          consumer.startMessageProcessor();
          consumers.put(consumerTag, consumer);
        } else
          throw new Exception("Consumer with consumer-tag '" + consumerTag + "' already exists");
        if (method.getNoWait())
          return;
        ConsumeOk consumeOk = new ConsumeOk();
        consumeOk.setConsumerTag(consumerTag);
        Frame frame = new Frame(Frame.TYPE_METHOD, channelNo, 0, null);
        amqpHandler.toPayload(frame, consumeOk);
        versionedConnection.send(frame);
      } catch (Exception e)
      {
        e.printStackTrace();
        amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, e.toString(), method));
      }
    }

    public void visit(ConsumeOk method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public void visit(Cancel method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      Consumer consumer = (Consumer) consumers.remove(method.getConsumerTag());
      if (consumer != null)
      {
        consumer.close();
        if (method.getNoWait())
          return;
        CancelOk cancelOk = new CancelOk();
        cancelOk.setConsumerTag(consumer.getConsumerTag());
        Frame frame = new Frame(Frame.TYPE_METHOD, channelNo, 0, null);
        amqpHandler.toPayload(frame, cancelOk);
        versionedConnection.send(frame);
      } else
        amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.NOT_FOUND, "Consumer with consumer-tag '" + method.getConsumerTag() + "' not found", method));
    }

    public void visit(CancelOk method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public void visit(Publish method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      currentMessage = new MessageWrap(method);
    }

    public void visit(Return method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public void visit(Deliver method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public void visit(Get method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      try
      {
        QueueReceiver receiver = ctx.queueManager.createQueueReceiver(method.getQueue(), versionedConnection.getActiveLogin(), null);
        QueuePullTransaction t = receiver.createTransaction(true);
        MessageEntry messageEntry = t.getMessage(0);
        if (messageEntry != null)
        {
          Delivery delivery = outboundTransformer.transform(messageEntry.getMessage());
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/getTransformedMessages, delivery=" + delivery);
          long deliveryTag = getNextDeliveryTag();
          GetOk getOk = new GetOk();
          getOk.setDeliveryTag(deliveryTag);
          getOk.setExchange("");
          getOk.setMessageCount((int) t.getAbstractQueue().getNumberQueueMessages());
          getOk.setRoutingKey(method.getQueue());
          Frame frame = new Frame(Frame.TYPE_METHOD, channelNo, 0, null);
          amqpHandler.toPayload(frame, getOk);
          versionedConnection.send(frame);
          frame = new Frame(Frame.TYPE_HEADER, channelNo, 0, null);
          ContentHeaderProperties contentHeaderProperties = delivery.getContentHeaderProperties();
          contentHeaderProperties.setClassId(getOk._getClassId());
          amqpHandler.toPayload(frame, contentHeaderProperties);
          versionedConnection.send(frame);
          byte[] body = delivery.getBody();
          int maxFrameSize = amqpHandler.getMaxFrameSize();
          int pos = 0;
          while (pos < body.length)
          {
            byte[] part = new byte[Math.min(body.length - pos, maxFrameSize)];
            System.arraycopy(body, pos, part, 0, part.length);
            pos += part.length;
            frame = new Frame(Frame.TYPE_BODY, channelNo, 0, null);
            frame.setPayload(part);
            frame.setSize(part.length);
            versionedConnection.send(frame);
          }
          if (method.getNoAck())
          {
            t.commit();
            receiver.close();
          } else
            unacked.add(new UnackedEntry(deliveryTag, receiver, t));
        } else
        {
          t.commit();
          receiver.close();
          Frame frame = new Frame(Frame.TYPE_METHOD, channelNo, 0, null);
          GetEmpty getEmpty = new GetEmpty();
          getEmpty.setReserved1("");
          amqpHandler.toPayload(frame, getEmpty);
          versionedConnection.send(frame);
        }
      } catch (Exception e)
      {
        amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Got exception during Get: " + e, method));
      }
    }

    public void visit(GetOk method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public void visit(GetEmpty method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public void visit(Ack method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      try
      {
        long deliveryTag = method.getDeliveryTag();
        boolean multiple = method.getMultiple();
        boolean found = false;
        for (ListIterator iter = unacked.listIterator(unacked.size()); iter.hasPrevious(); )
        {
          UnackedEntry unackedEntry = (UnackedEntry) iter.previous();
          if (found)
          {
            if (ctx.traceSpace.enabled)
              ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", acking " + unackedEntry.getDeliveryTag() + " due to muliple");
            if (unackedEntry.isGet)
              unackedEntry.ack();
            else
            {
              unackedEntry.getConsumer().ack(unackedEntry.getMessageIndex());
              unackedPrefetchedCount--;
              unackedPrefetchedSize -= unackedEntry.getSize();
            }
            iter.remove();
          } else
          {
            if (ctx.traceSpace.enabled)
              ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", checking " + unackedEntry.getDeliveryTag() + " vs " + deliveryTag);
            if (unackedEntry.getDeliveryTag() == deliveryTag)
            {
              if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", found!");
              found = true;
              if (unackedEntry.isGet)
                unackedEntry.ack();
              else
              {
                unackedEntry.getConsumer().ack(unackedEntry.getMessageIndex());
                unackedPrefetchedCount--;
                unackedPrefetchedSize -= unackedEntry.getSize();
              }
              iter.remove();
              if (!multiple)
                break;
            }
          }
        }
        if (!found)
          amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.NOT_FOUND, "Delivery tag not found: " + deliveryTag, method));
        else
        {
          dispatchWaitingSends();
        }
      } catch (Exception e)
      {
        amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Got exception during ack: " + e, method));
      }
    }

    public void visit(Reject method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      try
      {
        long deliveryTag = method.getDeliveryTag();
        boolean found = false;
        for (ListIterator iter = unacked.listIterator(unacked.size()); iter.hasPrevious(); )
        {
          UnackedEntry unackedEntry = (UnackedEntry) iter.previous();
          if (unackedEntry.getDeliveryTag() == deliveryTag)
          {
            found = true;
            if (method.getRequeue())
            {
              if (unackedEntry.isGet)
                unackedEntry.unack();
              else
              {
                unackedEntry.getConsumer().reject(unackedEntry.getMessageIndex());
                unackedPrefetchedCount--;
                unackedPrefetchedSize -= unackedEntry.getSize();
              }
            } else
            {
              if (unackedEntry.isGet)
                unackedEntry.ack();
              else
              {
                unackedEntry.getConsumer().ack(unackedEntry.getMessageIndex());
                unackedPrefetchedCount--;
                unackedPrefetchedSize -= unackedEntry.getSize();
              }
            }
            iter.remove();
            break;
          }
        }
        if (!found)
          amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.NOT_FOUND, "Delivery tag not found: " + deliveryTag, method));
        else
        {
          dispatchWaitingSends();
        }
      } catch (Exception e)
      {
        amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Got exception during ack: " + e, method));
      }
    }

    public void visit(RecoverAsync method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      try
      {
        doRecover(method.getRequeue());
      } catch (Exception e)
      {
        amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Got exception during recover-async: " + e, method));
      }
    }

    public void visit(Recover method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      try
      {
        doRecover(method.getRequeue());
        Frame frame = new Frame(Frame.TYPE_METHOD, channelNo, 0, null);
        amqpHandler.toPayload(frame, new RecoverOk());
        versionedConnection.send(frame);
      } catch (Exception e)
      {
        amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Got exception during recover: " + e, method));
      }
    }

    private void doRecover(boolean requeue) throws Exception
    {
      for (ListIterator iter = unacked.listIterator(unacked.size()); iter.hasPrevious(); )
      {
        UnackedEntry unackedEntry = (UnackedEntry) iter.previous();
        if (requeue)
        {
          if (unackedEntry.isGet)
            unackedEntry.unack();
          else
          {
            unackedEntry.getConsumer().reject(unackedEntry.getMessageIndex());
            unackedPrefetchedCount--;
            unackedPrefetchedSize -= unackedEntry.getSize();
          }
        } else
        {
          if (unackedEntry.isGet)
            unackedEntry.ack();
          else
          {
            unackedEntry.getConsumer().ack(unackedEntry.getMessageIndex());
            unackedPrefetchedCount--;
            unackedPrefetchedSize -= unackedEntry.getSize();
          }
        }
        iter.remove();
      }
      dispatchWaitingSends();
    }

    public void visit(RecoverOk method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public void visit(Nack method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.CHANNEL_ERROR, "Unexpected method received: " + method, method));
    }

    public String toString()
    {
      return ChannelHandler.this.toString() + "/BasicMethodVisitorImpl";
    }
  }

  private class TxMethodVisitorImpl implements TxMethodVisitor
  {
    public void visit(Select method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.NOT_IMPLEMENTED, "Method not implemented: " + method, method));
    }

    public void visit(SelectOk method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.NOT_IMPLEMENTED, "Method not implemented: " + method, method));
    }

    public void visit(Commit method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.NOT_IMPLEMENTED, "Method not implemented: " + method, method));
    }

    public void visit(CommitOk method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.NOT_IMPLEMENTED, "Method not implemented: " + method, method));
    }

    public void visit(Rollback method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.NOT_IMPLEMENTED, "Method not implemented: " + method, method));
    }

    public void visit(RollbackOk method)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit: " + method);
      amqpHandler.dispatch(new POSendChannelClose(channelNo, Constants.NOT_IMPLEMENTED, "Method not implemented: " + method, method));
    }

    public String toString()
    {
      return ChannelHandler.this.toString() + "/TxMethodVisitorImpl";
    }
  }

  private class Binding
  {
    String topicName = null;
    String queueName = null;
    int subscriberId = -1;
    QueueReceiver queueReceiver = null;
  }

  private class UnackedEntry
  {
    long deliveryTag;
    Consumer consumer;
    MessageIndex messageIndex;
    int size;
    boolean isGet = false;
    QueueReceiver queueReceiver = null;
    QueuePullTransaction queuePullTransaction = null;

    private UnackedEntry(long deliveryTag, Consumer consumer, MessageIndex messageIndex, int size)
    {
      this.deliveryTag = deliveryTag;
      this.consumer = consumer;
      this.messageIndex = messageIndex;
      this.size = size;
    }

    private UnackedEntry(long deliveryTag, QueueReceiver queueReceiver, QueuePullTransaction queuePullTransaction)
    {
      this.deliveryTag = deliveryTag;
      this.queueReceiver = queueReceiver;
      this.queuePullTransaction = queuePullTransaction;
      isGet = true;
    }

    public long getDeliveryTag()
    {
      return deliveryTag;
    }

    public Consumer getConsumer()
    {
      return consumer;
    }

    public MessageIndex getMessageIndex()
    {
      return messageIndex;
    }

    public int getSize()
    {
      return size;
    }

    public void ack()
    {
      try
      {
        queuePullTransaction.commit();
        queueReceiver.close();
      } catch (QueueException e)
      {
      }
    }

    public void unack()
    {
      try
      {
        queuePullTransaction.commit();
        queueReceiver.close();
      } catch (QueueException e)
      {
      }
    }
  }
}
