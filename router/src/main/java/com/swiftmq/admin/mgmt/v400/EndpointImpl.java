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

package com.swiftmq.admin.mgmt.v400;

import com.swiftmq.admin.mgmt.Endpoint;
import com.swiftmq.auth.ChallengeResponseFactory;
import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.jms.ReconnectListener;
import com.swiftmq.jms.SwiftMQConnection;
import com.swiftmq.mgmt.*;
import com.swiftmq.mgmt.protocol.ProtocolFactory;
import com.swiftmq.mgmt.protocol.v400.*;
import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.Dumpalizer;
import com.swiftmq.tools.requestreply.*;
import com.swiftmq.tools.timer.TimerEvent;
import com.swiftmq.tools.timer.TimerListener;
import com.swiftmq.tools.timer.TimerRegistry;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.util.SwiftUtilities;

import javax.jms.*;
import java.net.InetAddress;

public class EndpointImpl extends RequestServiceRegistry
    implements RequestHandler, TimerListener, MessageListener, Endpoint, ReconnectListener
{
  static final String MGMT_QUEUE = "swiftmqmgmt";
  static final LeaseRequest leaseRequest = new LeaseRequest();
  static final ProtocolFactory factory = new ProtocolFactory(new com.swiftmq.mgmt.protocol.v400.ProtocolFactory());

  String routerName = null;
  String[] actContext = null;
  QueueConnection connection = null;
  QueueSession senderSession = null;
  QueueSender sender = null;
  QueueSession receiverSession = null;
  QueueReceiver receiver = null;
  TemporaryQueue replyQueue = null;
  RequestRegistry requestRegistry = null;
  DataByteArrayInputStream dis = new DataByteArrayInputStream();
  DataByteArrayOutputStream dos = new DataByteArrayOutputStream();
  byte[] buffer = null;
  CommandRegistry commandRegistry = null;
  boolean createInternalCommands = false;
  long interval = 0;
  boolean started = false;
  boolean routeInfos = false;
  boolean subscriptionFilterEnabled = false;
  ConnectReply connectReply = null;

  public EndpointImpl(QueueConnection connection, QueueSession senderSession, QueueSender sender, QueueSession receiverSession, QueueReceiver receiver, TemporaryQueue replyQueue, RequestService requestService, boolean createInternalCommands) throws Exception
  {
    this.connection = connection;
    this.createInternalCommands = createInternalCommands;
    this.senderSession = senderSession;
    this.sender = sender;
    this.receiverSession = receiverSession;
    this.replyQueue = replyQueue;
    this.receiver = receiver;
    this.receiver.setMessageListener(this);
    addRequestService(requestService);
    requestRegistry = new RequestRegistry();
    requestRegistry.setRequestTimeoutEnabled(true);
    requestRegistry.setRequestHandler(this);
    commandRegistry = new CommandRegistry("Router Context", null);
    createCommands();
    createDefaultExecutor();
    ((SwiftMQConnection) connection).addReconnectListener(this);
  }

  private void createCommands()
  {
    if (createInternalCommands)
    {
      CommandExecutor getPropExecutor = new CommandExecutor()
      {
        public String[] execute(String[] context, Entity entity, String[] cmd)
        {
          String[] result = null;
          try
          {
            CommandReply reply = (CommandReply) requestRegistry.request(new CommandRequest(context, cmd, true));
            if (reply.isOk())
              result = reply.getResult();
            else
              result = new String[]{TreeCommands.ERROR, reply.getException().getMessage()};
          } catch (Exception e)
          {
          }
          return result;
        }
      };
      Command getPropCommand = new Command(TreeCommands.GET_CONTEXT_PROP, TreeCommands.GET_CONTEXT_PROP, "Internal use only!", true, getPropExecutor);
      commandRegistry.addCommand(getPropCommand);
      CommandExecutor getSubsExecutor = new CommandExecutor()
      {
        public String[] execute(String[] context, Entity entity, String[] cmd)
        {
          String[] result = null;
          try
          {
            CommandReply reply = (CommandReply) requestRegistry.request(new CommandRequest(context, cmd, true));
            if (reply.isOk())
              result = reply.getResult();
            else
              result = new String[]{TreeCommands.ERROR, reply.getException().getMessage()};
          } catch (Exception e)
          {
          }
          return result;
        }
      };
      Command getSubsCommand = new Command(TreeCommands.GET_CONTEXT_ENTITIES, TreeCommands.GET_CONTEXT_ENTITIES, "Internal use only!", true, getPropExecutor);
      commandRegistry.addCommand(getSubsCommand);
    }
    CommandExecutor ccExecutor = new CommandExecutor()
    {
      public String[] execute(String[] context, Entity entity, String[] cmd)
      {
        if (cmd.length != 2)
          return new String[]{TreeCommands.ERROR, "Invalid command, please try '" + TreeCommands.CHANGE_CONTEXT + " <context>'"};
        if (cmd[1].equals(".."))
        {
          actContext = SwiftUtilities.cutLast(actContext);
          return null;
        }
        String[] result = null;
        try
        {
          CommandReply reply = (CommandReply) requestRegistry.request(new CommandRequest(cmd[1].startsWith("/") ? null : actContext, cmd, true));
          if (reply.isOk())
          {
            result = reply.getResult();
            if (result == null)
            {
              if (cmd[1].startsWith("/"))
                actContext = SwiftUtilities.tokenize(cmd[1], "/");
              else
                actContext = SwiftUtilities.append(actContext, SwiftUtilities.tokenize(cmd[1], "/"));
            }
          } else
            result = new String[]{TreeCommands.ERROR, reply.getException().getMessage()};
        } catch (Exception e)
        {
        }
        return result;
      }
    };
    Command ccCommand = new Command(TreeCommands.CHANGE_CONTEXT, TreeCommands.CHANGE_CONTEXT + " <context>", "Change to Context <context>", true, ccExecutor);
    commandRegistry.addCommand(ccCommand);
    CommandExecutor lcExecutor = new CommandExecutor()
    {
      public String[] execute(String[] context, Entity entity, String[] cmd)
      {
        String[] result = null;
        try
        {
          String[] ctx = actContext;
          if (cmd.length == 2)
          {
            if (cmd[1].startsWith("/"))
              ctx = SwiftUtilities.tokenize(cmd[1], "/");
            else
              ctx = SwiftUtilities.append(actContext, SwiftUtilities.tokenize(cmd[1], "/"));
          }
          CommandReply reply = (CommandReply) requestRegistry.request(new CommandRequest(ctx, cmd, true));
          if (reply.isOk())
            result = reply.getResult();
          else
            result = new String[]{TreeCommands.ERROR, reply.getException().getMessage()};
        } catch (Exception e)
        {
        }
        return result;
      }
    };
    Command lcCommand = new Command(TreeCommands.DIR_CONTEXT, TreeCommands.DIR_CONTEXT + " [<context>]", "List the Content of <context>", true, lcExecutor);
    commandRegistry.addCommand(lcCommand);
    CommandExecutor authExecutor = new CommandExecutor()
    {
      public String[] execute(String[] context, Entity entity, String[] cmd)
      {
        if (cmd.length != 2)
          return new String[]{TreeCommands.ERROR, "Invalid command, please try '" + TreeCommands.AUTH + " <password>'"};
        try
        {
          authenticate(cmd[1]);
        } catch (Exception e)
        {
          return new String[]{TreeCommands.ERROR, e.getMessage()};
        }
        return null;
      }
    };
    Command authCommand = new Command(TreeCommands.AUTH, TreeCommands.AUTH + " <password>", "Authenticate Access", true, authExecutor);
    commandRegistry.addCommand(authCommand);
  }

  private void createDefaultExecutor()
  {
    CommandExecutor defaultExecutor = new CommandExecutor()
    {
      public String[] execute(String[] context, Entity entity, String[] command)
      {
        String[] result = null;
        try
        {
          CommandReply reply = (CommandReply) requestRegistry.request(new CommandRequest(actContext, command, false));
          if (reply.isOk())
            result = reply.getResult();
          else
            result = new String[]{TreeCommands.ERROR, reply.getException().getMessage()};
        } catch (Exception e)
        {
        }
        return result;
      }
    };
    commandRegistry.setDefaultCommand(defaultExecutor);
  }

  public void reconnected(String host, int port)
  {
    requestRegistry.cancelAllRequests(new TransportException("Reconnect occured: Request cancelled."));
  }

  public void connect(int connectId, String hostname, String toolName, boolean subscribeRouteInfos, boolean subscribeRouterConfig, boolean subscribeChangeEvents) throws Exception
  {
    ConnectRequest cr = new ConnectRequest(connectId, InetAddress.getLocalHost().getHostName(), toolName, subscribeRouteInfos, subscribeRouterConfig, subscribeChangeEvents);
    ConnectReply reply = (ConnectReply) request(cr);
    if (!reply.isOk())
      throw reply.getException();
    setRouterName(reply.getRouterName());
    startLease(reply.getLeaseTimeout() / 2);
    setStarted(true);
    connectReply = reply;
  }

  public boolean isAuthenticationRequired()
  {
    return connectReply.isAuthRequired();
  }

  public void authenticate(String password) throws Exception
  {
    ChallengeResponseFactory crFactory = (ChallengeResponseFactory) Class.forName(connectReply.getCrFactory()).newInstance();
    AuthReply reply = (AuthReply) request(new AuthRequest(crFactory.createResponse(connectReply.getChallenge(), password)));
    if (!reply.isOk())
      throw reply.getException();
  }

  public boolean isStarted()
  {
    return started;
  }

  public void setStarted(boolean started)
  {
    this.started = started;
  }

  public boolean isRouteInfos()
  {
    return routeInfos;
  }

  public void setRouteInfos(boolean routeInfos)
  {
    this.routeInfos = routeInfos;
  }

  public RequestRegistry getRequestRegistry()
  {
    return requestRegistry;
  }

  public void setRouterName(String routerName)
  {
    this.routerName = routerName;
  }

  public String getRouterName()
  {
    return routerName;
  }

  public String[] getActContext()
  {
    return actContext;
  }

  public void setActContext(String[] actContext)
  {
    this.actContext = actContext;
  }

  public boolean isSubscriptionFilterEnabled()
  {
    return false;
  }

  public void setSubscriptionFilterEnabled(boolean subscriptionFilterEnabled)
  {
  }

  public void contextShown(String[] context, boolean includeNextLevel)
  {
  }

  public void contextHidden(String[] context, boolean includeNextLevel)
  {
  }

  public void startLease(long interval)
  {
    this.interval = interval;
    TimerRegistry.Singleton().addTimerListener(interval, this);
  }

  public Reply request(Request request) throws Exception
  {
    return requestRegistry.request(request);
  }

  public synchronized void performRequest(Request request)
  {
    try
    {
      dos.rewind();
      Dumpalizer.dump(dos, request);
      BytesMessageImpl msg = new BytesMessageImpl();
      msg.writeBytes(dos.getBuffer(), 0, dos.getCount());
      msg.setJMSReplyTo(replyQueue);
      sender.send(msg);
    } catch (Exception e)
    {
      close();
    }
  }

  public String[] execute(String[] context, Entity entity, String[] command)
  {
    return commandRegistry.executeCommand(context, command);
  }

  public void performTimeAction(TimerEvent evt)
  {
    performRequest(leaseRequest);
  }

  public void onMessage(Message message)
  {
    try
    {
      BytesMessageImpl msg = (BytesMessageImpl) message;
      int len = (int) msg._getBodyLength();
      if (buffer == null || len > buffer.length)
        buffer = new byte[len];
      msg.readBytes(buffer);
      dis.reset();
      dis.setBuffer(buffer);
      Dumpable d = Dumpalizer.construct(dis, factory);
      if (d instanceof Reply)
        requestRegistry.setReply((Reply) d);
      else
        dispatch((Request) d);
    } catch (Exception e)
    {
      e.printStackTrace();
      close();
    }
  }

  public void close()
  {
    ((SwiftMQConnection) connection).removeReconnectListener(this);
    requestRegistry.cancelAllRequests(new TransportException("Request cancelled."));
    requestRegistry.close();
    TimerRegistry.Singleton().removeTimerListener(interval, this);
    try
    {
      receiver.close();
    } catch (JMSException e)
    {
    }
    try
    {
      sender.close();
    } catch (JMSException e)
    {
    }
    try
    {
      receiverSession.close();
    } catch (JMSException e)
    {
    }
    try
    {
      senderSession.close();
    } catch (JMSException e)
    {
    }
    try
    {
      replyQueue.delete();
    } catch (JMSException e)
    {
    }
  }
}
