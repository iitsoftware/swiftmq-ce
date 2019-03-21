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

import com.swiftmq.amqp.v100.generated.filter.filter_types.*;
import com.swiftmq.amqp.v100.generated.messaging.addressing.*;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.Accepted;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.DeliveryStateIF;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AddressIF;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AddressString;
import com.swiftmq.amqp.v100.generated.transactions.coordination.Coordinator;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TransactionalState;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TxnIdFactory;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TxnIdIF;
import com.swiftmq.amqp.v100.generated.transport.definitions.*;
import com.swiftmq.amqp.v100.generated.transport.definitions.Error;
import com.swiftmq.amqp.v100.generated.transport.performatives.*;
import com.swiftmq.amqp.v100.types.*;
import com.swiftmq.impl.amqp.SwiftletContext;
import com.swiftmq.impl.amqp.VersionedConnection;
import com.swiftmq.impl.amqp.accounting.AccountingProfile;
import com.swiftmq.impl.amqp.accounting.DestinationCollector;
import com.swiftmq.impl.amqp.accounting.DestinationCollectorCache;
import com.swiftmq.impl.amqp.amqp.v01_00_00.po.*;
import com.swiftmq.impl.amqp.amqp.v01_00_00.transaction.TransactionRegistry;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityAddException;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.auth.AuthenticationException;
import com.swiftmq.swiftlet.auth.ResourceLimitException;
import com.swiftmq.swiftlet.queue.MessageIndex;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.topic.TopicException;
import com.swiftmq.tools.collection.ArrayListTool;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.PipelineQueue;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.util.SwiftUtilities;
import org.apache.qpid.translator.Translator;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SessionHandler implements AMQPSessionVisitor
{
  SwiftletContext ctx = null;
  VersionedConnection versionedConnection = null;
  AMQPHandler amqpHandler = null;
  PipelineQueue pipelineQueue = null;
  SessionFrameVisitor frameVisitor = new SessionFrameVisitor();
  ExceptionVisitor exceptionVisitor = new ExceptionVisitor();
  boolean closed = false;
  boolean closeInProgress = false;
  Lock closeLock = new ReentrantLock();
  BeginFrame beginFrame = null;
  int channel;
  long maxHandle = 0;
  long maxMessageSize = 0;
  ArrayList handles = new ArrayList();
  Map remoteHandles = new HashMap();
  Map detachedLinks = new HashMap();
  Map unsettledIncomingDeliveries = new HashMap();
  Map unsettledOutgoingDeliveries = new HashMap();
  List outboundDeliveries = new ArrayList();
  TransactionRegistry transactionRegistry = null;
  Entity usage = null;
  DataByteArrayOutputStream dtagStream = new DataByteArrayOutputStream();

  long initialOutgoingId = 0;
  long initialOutgoingWindow = 0;
  long nextIncomingId = 0;
  long incomingWindow = 0;
  long nextOutgoingId = initialOutgoingId;
  long outgoingWindow = 0;
  long remoteIncomingWindow = 0;
  long remoteOutgoingWindow = 0;
  long deliveryId = initialOutgoingId;

  boolean windowChanged = false;

  Property propNextIncomingId = null;
  Property propIncomingWindow = null;
  Property propNextOutgoingId = null;
  Property propOutgoingWindow = null;
  Property propRemoteIncomingWindow = null;
  Property propRemoteOutgoingWindow = null;
  EntityList sourceLinkList = null;
  EntityList targetLinkList = null;

  public volatile int msgsReceived = 0;
  public volatile int msgsSent = 0;
  public volatile int totalMsgsReceived = 0;
  public volatile int totalMsgsSent = 0;

  boolean sessionDisabled = false;

  protected AccountingProfile accountingProfile = null;
  protected DestinationCollectorCache collectorCache = null;

  public SessionHandler(SwiftletContext ctx, VersionedConnection versionedConnection, AMQPHandler amqpHandler, BeginFrame beginFrame)
  {
    this.ctx = ctx;
    this.versionedConnection = versionedConnection;
    this.amqpHandler = amqpHandler;
    this.beginFrame = beginFrame;
    transactionRegistry = new TransactionRegistry(ctx, this);
    if (beginFrame.getNextOutgoingId() != null)
      nextIncomingId = beginFrame.getNextOutgoingId().getValue();
    if (beginFrame.getIncomingWindow() != null)
      remoteIncomingWindow = beginFrame.getIncomingWindow().getValue();
    if (beginFrame.getOutgoingWindow() != null)
      remoteOutgoingWindow = beginFrame.getOutgoingWindow().getValue();
    Entity connectionTemplate = versionedConnection.getConnectionTemplate();
    maxHandle = ((Long) connectionTemplate.getProperty("max-handle-number").getValue()).longValue();
    maxMessageSize = ((Long) connectionTemplate.getProperty("max-message-size").getValue()).longValue();
    incomingWindow = ((Integer) connectionTemplate.getProperty("incoming-window-size").getValue()).intValue();
    initialOutgoingWindow = ((Integer) connectionTemplate.getProperty("outgoing-window-size").getValue()).intValue();
    outgoingWindow = initialOutgoingWindow;
    pipelineQueue = new PipelineQueue(ctx.threadpoolSwiftlet.getPool(VersionedConnection.TP_SESSIONSVC), "SessionHandler", this);
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", created");
  }

  private long nextDeliveryId()
  {
    if (deliveryId == Long.MAX_VALUE)
      deliveryId = initialOutgoingId;
    else
      deliveryId++;
    return deliveryId;
  }

  private void removeDeliveries(ServerLink link, Map map)
  {
    for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); )
    {
      Map.Entry entry = (Map.Entry) iter.next();
      ServerLink l = (ServerLink) entry.getValue();
      if (link == l)
        iter.remove();
    }
  }

  private void closeAllLinks()
  {
    for (int i = 0; i < handles.size(); i++)
    {
      ServerLink link = (ServerLink) handles.get(i);
      if (link != null)
      {
        cleanupLink(link);
        link.close();
      }
    }
    for (Iterator iter = detachedLinks.entrySet().iterator(); iter.hasNext(); )
    {
      ServerLink link = (ServerLink) ((Map.Entry) iter.next()).getValue();
      if (link != null)
      {
        cleanupLink(link);
        link.close();
      }
    }
  }

  private Set toSet(AMQPArray capabilities) throws IOException
  {
    Set set = null;
    if (capabilities != null)
    {
      AMQPType[] t = capabilities.getValue();
      if (t != null && t.length > 0)
      {
        set = new HashSet();
        for (int i = 0; i < t.length; i++)
          set.add(((AMQPSymbol) t[i]).getValue());
      }
    }
    return set;
  }

  private void cleanupLink(ServerLink link)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", cleanupLink, link=" + link);
    if (link instanceof TargetLink)
    {
      versionedConnection.getActiveLogin().getResourceLimitGroup().decProducers();
      removeDeliveries(link, unsettledIncomingDeliveries);
      if (targetLinkList != null)
        targetLinkList.removeDynamicEntity(link);
    } else
    {
      versionedConnection.getActiveLogin().getResourceLimitGroup().decConsumers();
      removeDeliveries(link, unsettledOutgoingDeliveries);
      if (sourceLinkList != null)
        sourceLinkList.removeDynamicEntity(link);
    }
  }

  private void sendDetach(Handle handle, ErrorConditionIF condition, AMQPString description)
  {
    DetachFrame detachFrame = new DetachFrame(channel);
    detachFrame.setHandle(handle);
    detachFrame.setClosed(AMQPBoolean.TRUE);
    if (condition != null)
    {
      Error error = new Error();
      error.setCondition(condition);
      if (description != null)
        error.setDescription(description);
      detachFrame.setError(error);
    }
    versionedConnection.send(detachFrame);
  }

  private void sendFlow(TargetLink targetLink)
  {
    FlowFrame flowFrame = new FlowFrame(channel);
    flowFrame.setHandle(new Handle(targetLink.getHandle()));
    flowFrame.setAvailable(new AMQPUnsignedInt(0));
    flowFrame.setDrain(AMQPBoolean.FALSE);
    flowFrame.setNextIncomingId(new TransferNumber(nextIncomingId));
    flowFrame.setNextOutgoingId(new TransferNumber(nextOutgoingId));
    flowFrame.setLinkCredit(new AMQPUnsignedInt(targetLink.getLinkCredit()));
    flowFrame.setIncomingWindow(new AMQPUnsignedInt(incomingWindow));
    flowFrame.setOutgoingWindow(new AMQPUnsignedInt(outgoingWindow));
    if (targetLink.getDeliveryCount() != -1)
      flowFrame.setDeliveryCount(new SequenceNo(targetLink.getDeliveryCount()));
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", sendFlow, flowFrame: " + flowFrame);
    versionedConnection.send(flowFrame);
  }

  private void sendFlow(SourceLink sourceLink)
  {
    FlowFrame flowFrame = new FlowFrame(channel);
    flowFrame.setHandle(new Handle(sourceLink.getHandle()));
    flowFrame.setAvailable(new AMQPUnsignedInt(sourceLink.getAvailable()));
    flowFrame.setDeliveryCount(new SequenceNo(sourceLink.getDeliveryCountSnd()));
    flowFrame.setDrain(new AMQPBoolean(sourceLink.isDrain()));
    flowFrame.setNextOutgoingId(new TransferNumber(nextOutgoingId));
    flowFrame.setLinkCredit(new AMQPUnsignedInt(sourceLink.getLastReceivedLinkCredit()));
    flowFrame.setOutgoingWindow(new AMQPUnsignedInt(outgoingWindow));
    flowFrame.setIncomingWindow(new AMQPUnsignedInt(incomingWindow));
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", sendFlow, flowFrame: " + flowFrame);
    versionedConnection.send(flowFrame);
  }

  private void sendFlow()
  {
    FlowFrame flowFrame = new FlowFrame(channel);
    flowFrame.setIncomingWindow(new AMQPUnsignedInt(incomingWindow));
    flowFrame.setNextIncomingId(new TransferNumber(nextIncomingId));
    flowFrame.setOutgoingWindow(new AMQPUnsignedInt(outgoingWindow));
    flowFrame.setNextOutgoingId(new TransferNumber(nextOutgoingId));
    flowFrame.setDrain(AMQPBoolean.FALSE);
    flowFrame.setEcho(AMQPBoolean.FALSE);
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", sendFlow, flowFrame: " + flowFrame);
    versionedConnection.send(flowFrame);
  }

  private void settleInbound(long from, long to, DeliveryStateIF deliveryState) throws EndWithErrorException
  {
    if (from <= to)
    {
      long current = from;
      while (current <= to)
      {
        ServerLink link = (ServerLink) unsettledIncomingDeliveries.remove(current);
        if (link != null)
          link.settle(current, deliveryState);
        current++;
      }
    } else
      throw new SessionEndException(AmqpError.INVALID_FIELD, new AMQPString("settle inbound, from <= to!"));
  }

  private void settleOutbound(long from, long to, boolean settled, DeliveryStateIF deliveryState) throws EndWithErrorException
  {
    if (from <= to)
    {
      long current = from;
      while (current <= to)
      {
        settleOutbound(current, deliveryState);
        current++;
      }
      if (!settled)
      {
        DispositionFrame dispoFrame = new DispositionFrame(channel);
        dispoFrame.setRole(Role.SENDER);
        dispoFrame.setFirst(new DeliveryNumber(from));
        dispoFrame.setLast(new DeliveryNumber(to));
        dispoFrame.setSettled(AMQPBoolean.TRUE);
        dispoFrame.setState(deliveryState);
        versionedConnection.send(dispoFrame);
      }
    } else
      throw new SessionEndException(AmqpError.INVALID_FIELD, new AMQPString("settle outbound, from <= to!"));
  }

  public int getMsgsReceived()
  {
    int n = msgsReceived;
    msgsReceived = 0;
    return n;
  }

  public int getMsgsSent()
  {
    int n = msgsSent;
    msgsSent = 0;
    return n;
  }

  public int getTotalMsgsReceived()
  {
    return totalMsgsReceived;
  }

  public int getTotalMsgsSent()
  {
    return totalMsgsSent;
  }

  public void incMsgsSent(int n)
  {
    if (msgsSent == Integer.MAX_VALUE)
      msgsSent = 0;
    msgsSent += n;
    if (totalMsgsSent == Integer.MAX_VALUE)
      totalMsgsSent = 0;
    totalMsgsSent += n;
  }

  public void incMsgsReceived(int n)
  {
    if (msgsReceived == Integer.MAX_VALUE)
      msgsReceived = 0;
    msgsReceived += n;
    if (totalMsgsReceived == Integer.MAX_VALUE)
      totalMsgsReceived = 0;
    totalMsgsReceived += n;
  }

  public void setUsage(Entity usage)
  {
    this.usage = usage;
    if (usage != null)
    {
      propNextIncomingId = usage.getProperty("next-incoming-id");
      propNextOutgoingId = usage.getProperty("next-outgoing-id");
      propIncomingWindow = usage.getProperty("incoming-window");
      propOutgoingWindow = usage.getProperty("outgoing-window");
      propRemoteIncomingWindow = usage.getProperty("remote-incoming-window");
      propRemoteOutgoingWindow = usage.getProperty("remote-outgoing-window");
      sourceLinkList = (EntityList) usage.getEntity("sourcelinks");
      targetLinkList = (EntityList) usage.getEntity("targetlinks");
    }
  }

  private void flushCollectors()
  {
    if (accountingProfile != null && collectorCache != null)
    {
      collectorCache.flush(accountingProfile.getSource(), versionedConnection.getActiveLogin().getUserName(), versionedConnection.getActiveLogin().getClientId(), versionedConnection.getRemoteHostname(), AMQPHandler.VERSION);
      collectorCache.clear();
    }
  }

  public void settleOutbound(long deliveryId, DeliveryStateIF deliveryState) throws EndWithErrorException
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", settleOutbound, deliveryId=" + deliveryId + ", deliveryState=" + deliveryState);
    SourceLink link = (SourceLink) unsettledOutgoingDeliveries.remove(deliveryId);
    if (link != null)
      link.settle(deliveryId, deliveryState);
    outgoingWindow++;
  }

  public void applyDispositionFrame(DispositionFrame frame) throws EndWithErrorException
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", applyDispositionFrame, frame=" + frame);
    if (frame.getLast() == null)
      settleOutbound(frame.getFirst().getValue(), frame.getFirst().getValue(), frame.getSettled().getValue(), frame.getState());
    else
      settleOutbound(frame.getFirst().getValue(), frame.getLast().getValue(), frame.getSettled().getValue(), frame.getState());
  }

  public void sendOutboundDeliveries() throws EndWithErrorException
  {
    if (outboundDeliveries.size() > 0)
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", sendOutboundDeliveries, outboundDeliveries.size()=" + outboundDeliveries.size());
      OutboundDelivery[] ob = (OutboundDelivery[]) outboundDeliveries.toArray(new OutboundDelivery[outboundDeliveries.size()]);
      outboundDeliveries.clear();
      for (int i = 0; i < ob.length; i++)
      {
        OutboundDelivery _od = ob[i];
        doSendOneMessage(_od.sourceMessageProcessor, _od.sourceLink, _od.delivery, _od.restart);
      }
    }
  }

  private void doSendOneMessage(SourceMessageProcessor sourceMessageProcessor, SourceLink sourceLink, Delivery delivery, boolean restart) throws EndWithErrorException
  {
    try
    {
      if (remoteIncomingWindow > 0 && outgoingWindow > 0)
      {
        do
        {
          boolean wasFirstPacket = false;
          delivery.setMaxFrameSize(amqpHandler.getMaxFrameSize());
          TransferFrame frame = new TransferFrame(channel);
          frame.setHandle(new Handle(sourceLink.getHandle()));
          frame.setSettled(new AMQPBoolean(sourceLink.getSndSettleMode() == SenderSettleMode.SETTLED.getValue()));
          if (delivery.getCurrentPacketNumber() == 0)
          {
            if (ctx.traceSpace.enabled)
              ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", doSendOneMessage, amqpMessage=" + delivery.getAmqpMessage());
            long totalSize = delivery.getSize();
            wasFirstPacket = true;
            sourceLink.incDeliveryCountSnd();
            long dId = nextDeliveryId();
            frame.setDeliveryId(new DeliveryNumber(dId));
            frame.setDeliveryTag(createDeliveryTag(delivery));
            TxnIdIF currentTx = sourceLink.getCurrentTx();
            if (currentTx != null)
            {
              TransactionalState tState = new TransactionalState();
              tState.setTxnId(currentTx);
              frame.setState(tState);
              transactionRegistry.addToTransaction(currentTx, sourceLink, dId, delivery.getMessageIndex(), totalSize);
              totalSize = 0;
            }
            if (!frame.getSettled().getValue())
            {
              unsettledOutgoingDeliveries.put(dId, sourceLink);
              if (totalSize > 0)
                sourceLink.addUnsettled(dId, delivery.getMessageIndex(), totalSize);
              else
                sourceLink.addUnsettled(dId, delivery.getMessageIndex());
            } else
            {
              sourceLink.autoack(delivery.getMessageIndex());
              if (totalSize > 0)
              {
                DestinationCollector collector = sourceLink.getCollector();
                if (collector != null)
                  collector.incTotal(1, delivery.getSize());
              }
            }
            incMsgsSent(1);
          }
          delivery.getNextPacket(frame);
          // We may increase the outgoing window and send a flow before
          if (wasFirstPacket && outgoingWindow - delivery.getPredictedNumberPackets() < 0)
          {
            outgoingWindow += delivery.getPredictedNumberPackets();
            sendFlow();
            windowChanged = true;
          }
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", doSendOneMessage, remoteIncomingWindows=" + remoteIncomingWindow + ", outgoingWindow=" + outgoingWindow + ", sending message, wasFirstPacket=" + wasFirstPacket + ", maxSize=" + delivery.getMaxPayloadLength() + ", packetSize=" + frame.getPayload().length + ", predictedNumberPackets=" + delivery.getPredictedNumberPackets() + ", currentPacket=" + delivery.getCurrentPacketNumber() + ", hasMore=" + delivery.hasMore());
          versionedConnection.send(frame);
          if (!frame.getSettled().getValue())
            outgoingWindow--;
          nextOutgoingId++;
          remoteIncomingWindow--;
          if (!delivery.hasMore())
          {
            if (restart)
            {
              if (sourceLink.getLinkCredit() > 0)
                sourceLink.startMessageProcessor(sourceMessageProcessor);
              else
                sourceLink.clearMessageProcessor();
            }
            // If that was the last packet and outgoing window was increased for this message, we need to reset it and send another flow
            if (windowChanged)
            {
              outgoingWindow = initialOutgoingWindow;
              sendFlow();
            }
            break;
          }
        } while (remoteIncomingWindow > 0 && outgoingWindow > 0);
        if (delivery.hasMore())
        {
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", doSendOneMessage, remoteIncomingWindows=" + remoteIncomingWindow + ", outgoingWindow=" + outgoingWindow + ", has more but no window, storing message");
          outboundDeliveries.add(new OutboundDelivery(sourceMessageProcessor, sourceLink, delivery, restart));
        }
      } else
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", doSendOneMessage, no remote incoming window = " + remoteIncomingWindow + ", outgoingWindow=" + outgoingWindow + ", store for later transfer");
        outboundDeliveries.add(new OutboundDelivery(sourceMessageProcessor, sourceLink, delivery, restart));
      }
    } catch (IOException e)
    {
      throw new ConnectionEndException(AmqpError.INTERNAL_ERROR, new AMQPString("IOException during outbound send: " + e.getMessage()));
    } catch (QueueException e)
    {
      throw new ConnectionEndException(AmqpError.INTERNAL_ERROR, new AMQPString("QueueException during outbound send: " + e.getMessage()));
    }
  }

  private DeliveryTag createDeliveryTag(Delivery delivery) throws IOException
  {
    // Need to create a copy of the MessageIndex without the delivery count; otherwise it won't match on redeliveries
    MessageIndex messageIndex = delivery.getMessageIndex();
    MessageIndex deliveryTag = new MessageIndex(messageIndex.getId(), messageIndex.getPriority(), 0);
    deliveryTag.setTxId(0);
    dtagStream.rewind();
    deliveryTag.writeContent(dtagStream);
    byte b[] = new byte[dtagStream.getCount()];
    System.arraycopy(dtagStream.getBuffer(), 0, b, 0, b.length);
    return new DeliveryTag(b);
  }

  private void doSend(POSendMessages po) throws EndWithErrorException
  {
    try
    {
      SourceMessageProcessor processor = po.getSourceMessageProcessor();
      SourceLink sourceLink = processor.getSourceLink();
      if (processor.isBulkMode())
      {
        Delivery[] deliveries = processor.getTransformedMessages();
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, doSend, numberMessages=" + deliveries.length);
        for (int i = 0; i < deliveries.length; i++)
          doSendOneMessage(processor, sourceLink, deliveries[i], i == deliveries.length - 1);
      } else
        doSendOneMessage(processor, sourceLink, processor.getTransformedMessage(), true);
      if (sourceLink.isFlowAfterDrainRequired())
      {
        sourceLink.setFlowAfterDrainRequired(false);
        sourceLink.advanceDeliveryCount();
        sendFlow(sourceLink);
      }
    } catch (JMSException e)
    {
      throw new ConnectionEndException(AmqpError.INTERNAL_ERROR, new AMQPString("JMSException during outbound send: " + e.getMessage()));
    }
  }

  public VersionedConnection getVersionedConnection()
  {
    return versionedConnection;
  }

  public int getChannel()
  {
    return channel;
  }

  public void setChannel(int channel)
  {
    this.channel = channel;
  }

  public TransactionRegistry getTransactionRegistry()
  {
    return transactionRegistry;
  }

  public void startSession()
  {
    dispatch(new POSendBegin());
  }

  public void stopSession()
  {
    dispatch(new POSendEnd(null, null));
  }

  public void dispatch(POObject po)
  {
    if (!sessionDisabled)
      pipelineQueue.enqueue(po);
    else
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", Session disabled, PO discarded: " + po);
    }
  }

  public void collect(long lastCollect)
  {
    dispatch(new POSessionCollect(lastCollect));
  }

  public void startAccounting(AccountingProfile accountingProfile)
  {
    dispatch(new POSessionStartAccounting(accountingProfile));
  }

  public void stopAccounting()
  {
    dispatch(new POSessionStopAccounting());
  }

  public void flushAccounting()
  {
    dispatch(new POSessionFlushAccounting());
  }

  public void visit(POSessionFrameReceived po)
  {
    po.getFrame().accept(frameVisitor);
  }

  public void visit(POSendBegin po)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
    BeginFrame frame = new BeginFrame(channel);
    frame.setHandleMax(new Handle(maxHandle));
    frame.setNextOutgoingId(new TransferNumber(nextOutgoingId));
    frame.setIncomingWindow(new AMQPUnsignedInt(incomingWindow));
    frame.setOutgoingWindow(new AMQPUnsignedInt(outgoingWindow));
    frame.setRemoteChannel(new AMQPUnsignedShort(beginFrame.getChannel()));
    versionedConnection.send(frame);
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
  }

  public void visit(POSendMessages po)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
    SourceLink sourceLink = po.getSourceMessageProcessor().getSourceLink();
    if (sourceLink.isClosed())
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, POSendMessages, source link closed, return");
      return;
    }
    long linkCredit = sourceLink.getLinkCredit();
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, POSendMessages, linkCredit=" + linkCredit);
    if (linkCredit <= 0)
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, POSendMessages, parking PO ...");
      sourceLink.setWaitingPO(po);  // Will be released by the next flow frame
    } else
    {
      try
      {
        doSend(po);
      } catch (EndWithErrorException e)
      {
        e.accept(exceptionVisitor);
      }
    }
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
  }

  public void visit(POSendFlow po)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
    sendFlow(po.getTargetLink());
    po.getTargetLink().setFlowcontrolDelay(0);
    po.getTargetLink().setFlowcontrolTimer(null);
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
  }

  public void visit(POSessionCollect po)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
    if (usage != null)
    {
      try
      {
        if (((Long) propNextIncomingId.getValue()).longValue() != nextIncomingId)
          propNextIncomingId.setValue(new Long((long) nextIncomingId));
        if (((Long) propNextOutgoingId.getValue()).longValue() != nextOutgoingId)
          propNextOutgoingId.setValue(new Long((long) nextOutgoingId));
        if (((Long) propIncomingWindow.getValue()).longValue() != incomingWindow)
          propIncomingWindow.setValue(new Long((long) incomingWindow));
        if (((Long) propOutgoingWindow.getValue()).longValue() != outgoingWindow)
          propOutgoingWindow.setValue(new Long((long) outgoingWindow));
        if (((Long) propRemoteIncomingWindow.getValue()).longValue() != remoteIncomingWindow)
          propRemoteIncomingWindow.setValue(new Long((long) remoteIncomingWindow));
        if (((Long) propRemoteOutgoingWindow.getValue()).longValue() != remoteOutgoingWindow)
          propRemoteOutgoingWindow.setValue(new Long((long) remoteOutgoingWindow));
        for (int i = 0; i < handles.size(); i++)
        {
          ServerLink link = (ServerLink) handles.get(i);
          if (link != null)
            link.fillUsage();
        }
      } catch (Exception e)
      {
      }

    }
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
  }

  public void visit(POSessionStartAccounting po)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
    accountingProfile = po.getAccountingProfile();
    collectorCache = new DestinationCollectorCache(ctx, toString());
    for (int i = 0; i < handles.size(); i++)
    {
      ServerLink link = (ServerLink) handles.get(i);
      if (link != null)
        link.createCollector(accountingProfile, collectorCache);
    }
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
  }

  public void visit(POSessionStopAccounting po)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
    flushCollectors();
    for (int i = 0; i < handles.size(); i++)
    {
      ServerLink link = (ServerLink) handles.get(i);
      if (link != null)
        link.removeCollector();
    }
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
  }

  public void visit(POSessionFlushAccounting po)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
    flushCollectors();
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
  }

  public void visit(POSendEnd po)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
    EndFrame frame = new EndFrame(channel);
    if (po.getErrorCondition() != null)
    {
      com.swiftmq.amqp.v100.generated.transport.definitions.Error error = new Error();
      error.setCondition(po.getErrorCondition());
      if (po.getDescription() != null)
        error.setDescription(po.getDescription());
      frame.setError(error);
    }
    versionedConnection.send(frame);
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
  }

  public void visit(POCloseSession po)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
    closed = true;
    if (transactionRegistry != null)
      transactionRegistry.close();
    flushCollectors();
    accountingProfile = null;
    collectorCache = null;
    closeAllLinks();
    handles.clear();
    remoteHandles.clear();
    detachedLinks.clear();
    unsettledOutgoingDeliveries.clear();
    unsettledIncomingDeliveries.clear();
    outboundDeliveries.clear();
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
    sessionDisabled = false;
    Semaphore sem = new Semaphore();
    dispatch(new POCloseSession(sem));
    sem.waitHere();
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", close done");
  }

  public String toString()
  {
    return amqpHandler.toString() + "/SessionHandler, channel=" + channel;
  }

  private class SessionFrameVisitor extends FrameVisitorAdapter
  {

    private AddressIF convertAddress(AddressIF addressIF) throws Exception
    {
      if (addressIF instanceof AddressString)
      {
        AddressString oldAddr = (AddressString) addressIF;
        AddressString newAddr = new AddressString(SwiftUtilities.extractAMQPName(oldAddr.getValue()));
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", converted oldAddress=" + oldAddr.getValue() + " to newAddress=" + newAddr.getValue());
        return newAddr;
      }
      return addressIF;
    }

    private void attachSender(AttachFrame frame)
    {
      ReceiverSettleMode rcvSettleMode = frame.getRcvSettleMode();
      final TargetLink targetLink = new TargetLink(ctx, SessionHandler.this, frame.getName().getValue(), rcvSettleMode == null ? -1 : rcvSettleMode.getValue());
      if (targetLinkList != null)
      {
        Entity targetLinkUsage = targetLinkList.createEntity();
        targetLinkUsage.setName(frame.getName().getValue());
        targetLinkUsage.createCommands();
        targetLinkUsage.setDynamicObject(targetLink);
        try
        {
          targetLinkList.addEntity(targetLinkUsage);
        } catch (EntityAddException e)
        {
        }
        targetLink.setUsage(targetLinkUsage);
      }
      if (frame.getSource() != null)
      {
        frame.getSource().accept(new SourceVisitor()
        {
          public void visit(Source source)
          {
            targetLink.setRemoteAddress(source.getAddress());
          }
        });
      }
      if (frame.getTarget() != null)
      {
        frame.getTarget().accept(new TargetVisitor()
        {
          public void visit(Target target)
          {
            try
            {
              targetLink.setLocalAddress(convertAddress(target.getAddress()));
            } catch (Exception e)
            {
              new SessionEndException(AmqpError.INVALID_FIELD, new AMQPString("Exception concerning target address: " + e.toString())).accept(exceptionVisitor);
              return;
            }
          }

          public void visit(Coordinator coordinator)
          {
            targetLink.setCoordinator(true);
            coordinator.setCapabilities(targetLink.getOfferedCapabilitiesArray());
          }
        });
      }
      int handle = ArrayListTool.setFirstFreeOrExpand(handles, targetLink);
      targetLink.setHandle(handle);
      targetLink.setRemoteHandle(frame.getHandle().getValue());
      if (frame.getInitialDeliveryCount() != null)
        targetLink.setDeliveryCount(frame.getInitialDeliveryCount().getValue());
      remoteHandles.put(frame.getHandle().getValue(), targetLink);
      AttachFrame attachFrame = new AttachFrame(channel);
      try
      {
        attachFrame.setName(frame.getName());
        attachFrame.setHandle(new Handle(handle));
        if (maxMessageSize > 0)
          attachFrame.setMaxMessageSize(new AMQPUnsignedLong(maxMessageSize));
        attachFrame.setRole(Role.RECEIVER);
        attachFrame.setSource(frame.getSource());
        TargetIF targetIF = frame.getTarget();
        if (targetIF == null)
          throw new IOException("Attach frame does not contain a target");
        targetIF.accept(new TargetVisitor()
        {
          public void visit(Target target)
          {
            TerminusExpiryPolicy expiryPolicy = target.getExpiryPolicy();
            if (expiryPolicy != null &&
                (expiryPolicy.getValue().equals(TerminusExpiryPolicy.CONNECTION_CLOSE.getValue()) ||
                    expiryPolicy.getValue().equals(TerminusExpiryPolicy.NEVER.getValue())))
              target.setExpiryPolicy(TerminusExpiryPolicy.SESSION_END);
            if (target.getDynamic() != null)
              targetLink.setDynamic(target.getDynamic().getValue());
          }

          public void visit(Coordinator coordinator)
          {
          }
        });
        AMQPMap remoteUnsettled = frame.getUnsettled();
        if (remoteUnsettled != null)
          targetLink.setRemoteUnsettled(remoteUnsettled.getValue());
        versionedConnection.getActiveLogin().getResourceLimitGroup().incProducers();
        targetLink.verifyLocalAddress();
        targetIF.accept(new TargetVisitor()
        {
          public void visit(Target target)
          {
            if (targetLink.isDynamic())
              target.setAddress(new AddressString(targetLink.getQueueName()));
          }

          public void visit(Coordinator coordinator)
          {
          }
        });
        attachFrame.setTarget(targetIF);
        if (accountingProfile != null && collectorCache != null)
          targetLink.createCollector(accountingProfile, collectorCache);
        targetLink.setOfferedCapabilities(toSet(frame.getOfferedCapabilities()));
        targetLink.setDesiredCapabilities(toSet(frame.getDesiredCapabilities()));
        versionedConnection.send(attachFrame);
        sendFlow(targetLink);
      } catch (InvalidSelectorException e)
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", exception: " + e);
        attachFrame.setTarget(null);
        versionedConnection.send(attachFrame);
        sendDetach(attachFrame.getHandle(), AmqpError.INTERNAL_ERROR, new AMQPString(e.getMessage()));
        handles.set(handle, null);
        remoteHandles.remove(frame.getHandle().getValue());
      } catch (QueueException e)
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", exception: " + e);
        attachFrame.setTarget(null);
        versionedConnection.send(attachFrame);
        sendDetach(attachFrame.getHandle(), AmqpError.NOT_FOUND, new AMQPString(e.getMessage()));
        handles.set(handle, null);
        remoteHandles.remove(frame.getHandle().getValue());
      } catch (TopicException e)
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", exception: " + e);
        attachFrame.setTarget(null);
        versionedConnection.send(attachFrame);
        sendDetach(attachFrame.getHandle(), AmqpError.INTERNAL_ERROR, new AMQPString(e.getMessage()));
        handles.set(handle, null);
        remoteHandles.remove(frame.getHandle().getValue());
      } catch (AuthenticationException e)
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", exception: " + e);
        attachFrame.setTarget(null);
        versionedConnection.send(attachFrame);
        sendDetach(attachFrame.getHandle(), AmqpError.UNAUTHORIZED_ACCESS, new AMQPString(e.getMessage()));
        handles.set(handle, null);
        remoteHandles.remove(frame.getHandle().getValue());
      } catch (ResourceLimitException e)
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", exception: " + e);
        versionedConnection.getActiveLogin().getResourceLimitGroup().decProducers();
        attachFrame.setTarget(null);
        versionedConnection.send(attachFrame);
        sendDetach(attachFrame.getHandle(), AmqpError.RESOURCE_LIMIT_EXCEEDED, new AMQPString(e.getMessage()));
        handles.set(handle, null);
        remoteHandles.remove(frame.getHandle().getValue());
      } catch (IOException e)
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", exception: " + e);
        attachFrame.setTarget(null);
        versionedConnection.send(attachFrame);
        sendDetach(attachFrame.getHandle(), AmqpError.DECODE_ERROR, new AMQPString(e.getMessage()));
        handles.set(handle, null);
        remoteHandles.remove(frame.getHandle().getValue());
      }
    }

    private void attachReceiver(AttachFrame frame)
    {
      SenderSettleMode sndSettleMode = frame.getSndSettleMode();
      final SourceLink sourceLink = new SourceLink(ctx, SessionHandler.this, frame.getName().getValue(), sndSettleMode == null ? -1 : sndSettleMode.getValue());
      if (sourceLinkList != null)
      {
        Entity sourceLinkUsage = sourceLinkList.createEntity();
        sourceLinkUsage.setName(frame.getName().getValue());
        sourceLinkUsage.createCommands();
        sourceLinkUsage.setDynamicObject(sourceLink);
        try
        {
          sourceLinkList.addEntity(sourceLinkUsage);
        } catch (EntityAddException e)
        {
        }
        sourceLink.setUsage(sourceLinkUsage);
      }
      if (frame.getSource() != null)
      {
        frame.getSource().accept(new SourceVisitor()
        {
          public void visit(Source source)
          {
            try
            {
              sourceLink.setLocalAddress(convertAddress(source.getAddress()));
            } catch (Exception e)
            {
              new SessionEndException(AmqpError.INVALID_FIELD, new AMQPString("Exception concerning source address: " + e.toString())).accept(exceptionVisitor);
              return;
            }
            if (source.getDefaultOutcome() != null)
              sourceLink.setDefaultOutcome(source.getDefaultOutcome());
            if (source.getDurable() != null)
              sourceLink.setDurability(source.getDurable());
            if (source.getExpiryPolicy() != null)
              sourceLink.setExpiryPolicy(source.getExpiryPolicy());
            if (source.getDynamic() != null)
              sourceLink.setDynamic(source.getDynamic().getValue());
            if (source.getFilter() != null)
            {
              try
              {
                Map m = source.getFilter().getValue();
                if (m != null)
                {
                  final ExceptionHolder exceptionHolder = new ExceptionHolder();
                  for (Iterator iter = m.entrySet().iterator(); iter.hasNext(); )
                  {
                    FilterIF filter = FilterFactory.create((AMQPType) ((Map.Entry) iter.next()).getValue());
                    filter.accept(new FilterVisitor()
                    {
                      public void visit(NoLocalFilter noLocalFilter)
                      {
                        if (ctx.traceSpace.enabled)
                          ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), sourceLink.toString() + ", setting noLocal filter to: " + true);
                        sourceLink.setNoLocal(true);
                      }

                      public void visit(SelectorFilter jmsSelectorFilter)
                      {
                        if (amqpHandler.isApacheSelectors())
                        {
                          try
                          {
                            if (ctx.traceSpace.enabled)
                              ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), sourceLink.toString() + ", setting JMS selector filter, input filter: " + jmsSelectorFilter.getValue());
                            String translated = new Translator(new StringReader(jmsSelectorFilter.getValue())).translate(Translator.AMQP_TO_JMS);
                            if (ctx.traceSpace.enabled)
                              ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), sourceLink.toString() + ", setting JMS selector filter, translated filter: " + translated);
                            sourceLink.setMessageSelector(translated);
                          } catch (IOException e)
                          {
                            exceptionHolder.endWithErrorException = new SessionEndException(AmqpError.INVALID_FIELD, new AMQPString("Exception concerning message filter: " + e.toString()));
                          }
                        } else
                        {
                          if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), sourceLink.toString() + ", setting JMS selector filter: " + jmsSelectorFilter.getValue());
                          sourceLink.setMessageSelector(jmsSelectorFilter.getValue());
                        }
                      }
                    });
                    if (exceptionHolder.endWithErrorException != null)
                    {
                      exceptionHolder.endWithErrorException.accept(exceptionVisitor);
                      return;
                    }
                  }
                }
              } catch (Exception e)
              {
                new SessionEndException(AmqpError.INVALID_FIELD, new AMQPString("Exception concerning message filter: " + e.toString())).accept(exceptionVisitor);
                return;
              }
            }
          }
        });
      }
      if (frame.getTarget() != null)
      {
        frame.getTarget().accept(new TargetVisitor()
        {
          public void visit(Target target)
          {
            sourceLink.setRemoteAddress(target.getAddress());
          }

          public void visit(Coordinator coordinator)
          {
          }
        });
      }
      int handle = ArrayListTool.setFirstFreeOrExpand(handles, sourceLink);
      sourceLink.setHandle(handle);
      sourceLink.setRemoteHandle(frame.getHandle().getValue());
      remoteHandles.put(frame.getHandle().getValue(), sourceLink);
      AttachFrame attachFrame = new AttachFrame(channel);
      attachFrame.setName(frame.getName());
      attachFrame.setHandle(new Handle(handle));
      attachFrame.setRole(Role.SENDER);
      attachFrame.setTarget(frame.getTarget());
      attachFrame.setInitialDeliveryCount(new SequenceNo(sourceLink.getDeliveryCountSnd()));
      try
      {
        AMQPMap remoteUnsettled = frame.getUnsettled();
        if (remoteUnsettled != null)
          sourceLink.setRemoteUnsettled(remoteUnsettled.getValue());
        versionedConnection.getActiveLogin().getResourceLimitGroup().incConsumers();
        sourceLink.verifyLocalAddress();
        if (accountingProfile != null && collectorCache != null)
          sourceLink.createCollector(accountingProfile, collectorCache);
        attachFrame.setSndSettleMode(new SenderSettleMode(sourceLink.getSndSettleMode()));
        SourceIF sourceIF = frame.getSource();
        if (sourceIF != null)
        {
          sourceIF.accept(new SourceVisitor()
          {
            public void visit(Source source)
            {
              source.setExpiryPolicy(sourceLink.getExpiryPolicy());
              source.setDurable(sourceLink.getDurability());
              source.setOutcomes(sourceLink.getSupportedOutcomes());
              source.setDefaultOutcome(sourceLink.getDefaultOutcome());
              if (sourceLink.isDynamic())
                source.setAddress(new AddressString(sourceLink.getQueueName()));
            }
          });
          attachFrame.setSource(sourceIF);
        } else
        {
          Source source = new Source();
          source.setAddress(sourceLink.getLocalAddress());
          source.setExpiryPolicy(sourceLink.getExpiryPolicy());
          source.setDurable(sourceLink.getDurability());
          source.setDefaultOutcome(sourceLink.getDefaultOutcome());
          source.setDynamic(new AMQPBoolean(sourceLink.isDynamic()));
          attachFrame.setSource(source);
        }

        sourceLink.setOfferedCapabilities(toSet(frame.getOfferedCapabilities()));
        sourceLink.setDesiredCapabilities(toSet(frame.getDesiredCapabilities()));
        versionedConnection.send(attachFrame);
        if (remoteUnsettled != null)
        {
          Map<AMQPType, AMQPType> map = remoteUnsettled.getValue();
          for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); )
          {
            Map.Entry entry = (Map.Entry) iter.next();
            AMQPBinary deliveryTag = (AMQPBinary) entry.getKey();
            TransferFrame tf = new TransferFrame(channel);
            tf.setHandle(new Handle(sourceLink.getHandle()));
            tf.setSettled(new AMQPBoolean(true));
            tf.setResume(new AMQPBoolean(true));
            tf.setDeliveryId(new DeliveryNumber(nextDeliveryId()));
            tf.setDeliveryTag(new DeliveryTag(deliveryTag.getValue()));
            tf.setState(new Accepted());
            versionedConnection.send(tf);
            nextOutgoingId++;
          }
        }
      } catch (InvalidSelectorException e)
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", exception: " + e);
        attachFrame.setSource(null);
        versionedConnection.send(attachFrame);
        sendDetach(attachFrame.getHandle(), AmqpError.INVALID_FIELD, new AMQPString(e.getMessage()));
        handles.set(handle, null);
        remoteHandles.remove(frame.getHandle().getValue());
      } catch (QueueException e)
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", exception: " + e);
        attachFrame.setSource(null);
        versionedConnection.send(attachFrame);
        sendDetach(attachFrame.getHandle(), AmqpError.NOT_FOUND, new AMQPString(e.getMessage()));
        handles.set(handle, null);
        remoteHandles.remove(frame.getHandle().getValue());
      } catch (TopicException e)
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", exception: " + e);
        attachFrame.setSource(null);
        versionedConnection.send(attachFrame);
        sendDetach(attachFrame.getHandle(), AmqpError.INTERNAL_ERROR, new AMQPString(e.getMessage()));
        handles.set(handle, null);
        remoteHandles.remove(frame.getHandle().getValue());
      } catch (AuthenticationException e)
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", exception: " + e);
        attachFrame.setSource(null);
        versionedConnection.send(attachFrame);
        sendDetach(attachFrame.getHandle(), AmqpError.UNAUTHORIZED_ACCESS, new AMQPString(e.getMessage()));
        handles.set(handle, null);
        remoteHandles.remove(frame.getHandle().getValue());
      } catch (ResourceLimitException e)
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", exception: " + e);
        versionedConnection.getActiveLogin().getResourceLimitGroup().decConsumers();
        attachFrame.setTarget(null);
        versionedConnection.send(attachFrame);
        sendDetach(attachFrame.getHandle(), AmqpError.RESOURCE_LIMIT_EXCEEDED, new AMQPString(e.getMessage()));
        handles.set(handle, null);
        remoteHandles.remove(frame.getHandle().getValue());
      } catch (IOException e)
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", exception: " + e);
        attachFrame.setSource(null);
        versionedConnection.send(attachFrame);
        sendDetach(attachFrame.getHandle(), AmqpError.DECODE_ERROR, new AMQPString(e.getMessage()));
        handles.set(handle, null);
        remoteHandles.remove(frame.getHandle().getValue());
      }
    }

    public void visit(AttachFrame frame)
    {
      if (sessionDisabled)
        return;
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, AttachFrame: " + frame);
      if (frame.getRole().getValue() == Role.SENDER.getValue())
        attachSender(frame);
      else
        attachReceiver(frame);
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, AttachFrame done");
    }

    public void visit(FlowFrame frame)
    {
      if (sessionDisabled)
        return;
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, FlowFrame: " + frame);
      if (frame.getNextOutgoingId() != null)
        nextIncomingId = frame.getNextOutgoingId().getValue();
      if (frame.getIncomingWindow() != null)
        remoteIncomingWindow = frame.getIncomingWindow().getValue();
      if (frame.getOutgoingWindow() != null)
        remoteOutgoingWindow = frame.getOutgoingWindow().getValue();
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, FlowFrame, old remoteIncomingWindow=" + remoteIncomingWindow + ", nextIncomingId=" + nextIncomingId + ", nextOutgoingId=" + nextOutgoingId);
      if (frame.getNextIncomingId() != null)
        remoteIncomingWindow = frame.getNextIncomingId().getValue() + remoteIncomingWindow - nextOutgoingId;
      else
        remoteIncomingWindow = initialOutgoingId + remoteIncomingWindow - nextOutgoingId;
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, FlowFrame, new remoteIncomingWindow=" + remoteIncomingWindow);
      try
      {
        sendOutboundDeliveries();
      } catch (EndWithErrorException e)
      {
        e.accept(exceptionVisitor);
        return;
      }
      if (frame.getHandle() != null)
      {
        ServerLink link = (ServerLink) remoteHandles.get(frame.getHandle().getValue());
        if (link != null)
        {
          if (link instanceof SourceLink)
          {
            SourceLink sourceLink = (SourceLink) link;
            Fields fields = frame.getProperties();
            if (fields != null)
            {
              try
              {
                Map props = fields.getValue();
                AMQPType t = (AMQPType) props.get(new AMQPSymbol("txn-id"));
                if (t != null)
                  sourceLink.setCurrentTx(TxnIdFactory.create(t));
              } catch (Exception e)
              {
                new SessionEndException(AmqpError.INVALID_FIELD, new AMQPString(e.toString())).accept(exceptionVisitor);
                return;
              }
            }
            AMQPBoolean drain = frame.getDrain();
            if (drain != null)
              sourceLink.setDrain(drain.getValue());
            AMQPUnsignedInt linkCredit = frame.getLinkCredit();
            if (linkCredit != null)
              sourceLink.setLinkCredit(linkCredit.getValue());
            AMQPUnsignedInt dcount = frame.getDeliveryCount();
            if (dcount != null)
              sourceLink.setDeliveryCountRcv(dcount.getValue());
            boolean echoB = false;
            AMQPBoolean echo = frame.getEcho();
            if (echo != null)
              echoB = echo.getValue();
            POSendMessages po = (POSendMessages) sourceLink.getWaitingPO();
            if (po != null && sourceLink.getLinkCredit() > 0)
            {
              try
              {
                doSend(po);
              } catch (EndWithErrorException e)
              {
                e.accept(exceptionVisitor);
                return;
              }
              sourceLink.setWaitingPO(null);
            }
            if (!sourceLink.isMessageProcessorRunning())
            {
              try
              {
                if (sourceLink.getLinkCredit() > 0)
                  sourceLink.startMessageProcessor();
              } catch (QueueException e)
              {
                new SessionEndException(AmqpError.INTERNAL_ERROR, new AMQPString(e.toString())).accept(exceptionVisitor);
                return;
              }
            }
            if (echoB || sourceLink.getAvailable() == 0 && sourceLink.isFlowAfterDrainRequired())
            {
              if (sourceLink.isFlowAfterDrainRequired())
              {
                sourceLink.advanceDeliveryCount();
                sourceLink.setFlowAfterDrainRequired(false);
              }
              sendFlow(sourceLink);
            }
          }
        }
      }
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, FlowFrame done");
    }

    public void visit(TransferFrame frame)
    {
      if (sessionDisabled)
        return;
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, TransferFrame: " + frame);
      try
      {
        TargetLink targetLink = (TargetLink) remoteHandles.get(frame.getHandle().getValue());
        if (targetLink != null)
        {
          if (frame.getAborted() != null && frame.getAborted().getValue())
          {
            if (ctx.traceSpace.enabled)
              ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, TransferFrame: " + frame + " *** ABORTED ***");
            return;
          }
          // Must be done one the first frame of a message
          if (targetLink.getCurrentMessageSize() == 0)
          {
            incMsgsReceived(1);
          }
          if (frame.getResume() != null && frame.getResume().getValue())
          {
            if (ctx.traceSpace.enabled)
              ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, TransferFrame: " + frame + " *** RESUMED ***");
            return;
          }
          boolean atLeastOnce = targetLink.getRcvSettleMode() == ReceiverSettleMode.FIRST.getValue();
          boolean requiresFlow = targetLink.addTransferFrame(frame);
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, TransferFrame: " + frame + ", requires flow: " + requiresFlow);
          if (targetLink.getCurrentMessageSize() > maxMessageSize)
          {
            new LinkEndException(targetLink, LinkError.MESSAGE_SIZE_EXCEEDED, new AMQPString("message size (" + targetLink.getCurrentMessageSize() + ") > max message size (" + maxMessageSize + ")")).accept(exceptionVisitor);
            return;
          }
          if (frame.getMore() == null || !frame.getMore().getValue())
          {
            if (ctx.traceSpace.enabled)
              ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, TransferFrame: " + frame + ", last or only frame, do settlement:");
            boolean settled = false;
            if (frame.getSettled() != null)
              settled = frame.getSettled().getValue();
            if (!settled)
            {
              DispositionFrame dispoFrame = new DispositionFrame(channel);
              dispoFrame.setRole(Role.RECEIVER);
              dispoFrame.setFirst(targetLink.getLastDeliveryId());
              DeliveryStateIF transferState = frame.getState();
              if (transferState != null && transferState instanceof TransactionalState)
              {
                TransactionalState dispoState = new TransactionalState();
                dispoState.setTxnId(((TransactionalState) transferState).getTxnId());
                dispoState.setOutcome(new Accepted());
                dispoFrame.setState(dispoState);
                dispoFrame.setSettled(AMQPBoolean.TRUE);
              } else
              {
                dispoFrame.setState(targetLink.getLastDeliveryState());
                dispoFrame.setSettled(new AMQPBoolean(atLeastOnce));
                long deliveryId = targetLink.getLastDeliveryId().getValue();
                if (!atLeastOnce)
                  unsettledIncomingDeliveries.put(deliveryId, targetLink);
              }
              versionedConnection.send(dispoFrame);
            }
            if (requiresFlow)
            {
              if (targetLink.getFlowcontrolDelay() > 100)
              {
                if (targetLink.getFlowcontrolTimer() == null)
                {
                  if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, TransferFrame, register fctimer for fcdelay=" + targetLink.getFlowcontrolDelay() + ", targetLink=" + targetLink);
                  FlowcontrolTimer flowcontrolTimer = new FlowcontrolTimer(SessionHandler.this, targetLink);
                  targetLink.setFlowcontrolTimer(flowcontrolTimer);
                  ctx.timerSwiftlet.addInstantTimerListener(targetLink.getFlowcontrolDelay(), flowcontrolTimer);
                }
              } else
                sendFlow(targetLink);
            }
          }
          nextIncomingId++;
          incomingWindow--;
          if (incomingWindow == 0)
          {
            incomingWindow = ((Integer) versionedConnection.getConnectionTemplate().getProperty("incoming-window-size").getValue()).intValue();
            sendFlow();
          }
        } else
          throw new SessionEndException(SessionError.ERRANT_LINK, new AMQPString("Link for handle " + frame.getHandle().getValue() + " unknown"));
      } catch (EndWithErrorException e)
      {
        e.accept(exceptionVisitor);
      }
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, TransferFrame done");
    }

    public void visit(DispositionFrame frame)
    {
      if (sessionDisabled)
        return;
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, DispositionFrame: " + frame);
      try
      {
        if (frame.getRole().getValue() == Role.SENDER.getValue())
        {
          if (frame.getSettled().getValue())
          {
            long from = frame.getFirst().getValue();
            long to = frame.getFirst().getValue();
            if (frame.getLast() != null)
              to = frame.getLast().getValue();
            settleInbound(from, to, frame.getState());
          }
        } else
        {
          DeliveryStateIF state = frame.getState();
          if (state != null && state instanceof TransactionalState)
            transactionRegistry.addToTransaction(((TransactionalState) state).getTxnId(), frame);
          else
          {
            applyDispositionFrame(frame);
            sendOutboundDeliveries();
          }
        }
      } catch (EndWithErrorException e)
      {
        e.accept(exceptionVisitor);
      }

      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, DispositionFrame done");
    }

    public void visit(DetachFrame frame)
    {
      if (sessionDisabled)
        return;
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, DetachFrame: " + frame);
      ServerLink link = (ServerLink) remoteHandles.get(frame.getHandle().getValue());
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", link: " + link);
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", unsettledIncomingDeliveries: " + unsettledIncomingDeliveries);
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", unsettledOutgoingDeliveries: " + unsettledOutgoingDeliveries);
      if (link != null)
      {
        if (frame.getClosed() != null && frame.getClosed().getValue() == AMQPBoolean.TRUE.getValue())
        {
          remoteHandles.remove(frame.getHandle().getValue());
          handles.set(link.getHandle(), null);
          cleanupLink(link);
          link.close();
        } else
          detachedLinks.put(link.getName(), link);
        DetachFrame detachFrame = new DetachFrame(channel);
        detachFrame.setHandle(new Handle(link.getHandle()));
        detachFrame.setClosed(link.isClosed() ? AMQPBoolean.TRUE : AMQPBoolean.FALSE);
        versionedConnection.send(detachFrame);
      }
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", unsettledIncomingDeliveries: " + unsettledIncomingDeliveries);
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", unsettledOutgoingDeliveries: " + unsettledOutgoingDeliveries);
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, DetachFrame done");
    }

    public String toString()
    {
      return SessionHandler.this.toString() + "/SessionFrameVisitor";
    }
  }

  private class ExceptionVisitor implements EndWithErrorExceptionVisitor
  {
    public void visit(LinkEndException exception)
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", " + exception);
      ctx.logSwiftlet.logError(ctx.amqpSwiftlet.getName(), toString() + ", " + exception);
      ServerLink link = (ServerLink) remoteHandles.remove(exception.getLink().getRemoteHandle());
      if (link != null)
      {
        handles.set(link.getHandle(), null);
        cleanupLink(link);
        link.close();
        sendDetach(new Handle(link.getHandle()), exception.getCondition(), exception.getDescription());
      }
    }

    public void visit(SessionEndException exception)
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", " + exception);
      ctx.logSwiftlet.logError(ctx.amqpSwiftlet.getName(), toString() + ", " + exception);
      dispatch(new POSendEnd(exception.getCondition(), exception.getDescription()));
      sessionDisabled = true;
    }

    public void visit(ConnectionEndException exception)
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", " + exception);
      ctx.logSwiftlet.logError(ctx.amqpSwiftlet.getName(), toString() + ", " + exception);
      amqpHandler.dispatch(new POSendClose(exception.getCondition(), exception.getDescription()));
    }

    public String toString()
    {
      return SessionHandler.this.toString() + "/ExceptionVisitor";
    }
  }

  private class OutboundDelivery
  {
    SourceMessageProcessor sourceMessageProcessor;
    SourceLink sourceLink;
    Delivery delivery;
    boolean restart;

    private OutboundDelivery(SourceMessageProcessor sourceMessageProcessor, SourceLink sourceLink, Delivery delivery, boolean restart)
    {
      this.sourceMessageProcessor = sourceMessageProcessor;
      this.sourceLink = sourceLink;
      this.delivery = delivery;
      this.restart = restart;
    }
  }

  private class ExceptionHolder
  {
    EndWithErrorException endWithErrorException = null;
  }
}
