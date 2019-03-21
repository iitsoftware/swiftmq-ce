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

package com.swiftmq.amqp.v100.mgmt.v400;

import com.swiftmq.admin.mgmt.Endpoint;
import com.swiftmq.amqp.v100.client.*;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AddressIF;
import com.swiftmq.amqp.v100.generated.messaging.message_format.Data;
import com.swiftmq.amqp.v100.generated.messaging.message_format.Properties;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.auth.ChallengeResponseFactory;
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

import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EndpointImpl extends RequestServiceRegistry
    implements RequestHandler, TimerListener, MessageAvailabilityListener, Endpoint
{
  static final LeaseRequest leaseRequest = new LeaseRequest();
  static final ProtocolFactory factory = new ProtocolFactory(new com.swiftmq.mgmt.protocol.v400.ProtocolFactory());

  String routerName = null;
  String[] actContext = null;
  Connection connection = null;
  Session session = null;
  Producer producer = null;
  Consumer consumer = null;
  AddressIF replyAddress = null;
  RequestRegistry requestRegistry = null;
  DataByteArrayInputStream dis = new DataByteArrayInputStream();
  DataByteArrayOutputStream dos = new DataByteArrayOutputStream();
  byte[] buffer = null;
  CommandRegistry commandRegistry = null;
  boolean createInternalCommands = false;
  long interval = 0;
  boolean started = false;
  boolean routeInfos = false;
  ConnectReply connectReply = null;
  ExecutorService pollerService = Executors.newSingleThreadExecutor();
  Poller poller = null;

  public EndpointImpl(Connection connection, Session session, Producer producer, Consumer consumer, AddressIF replyAddress, RequestService requestService, boolean createInternalCommands) throws Exception
  {
    this.connection = connection;
    this.createInternalCommands = createInternalCommands;
    this.session = session;
    this.producer = producer;
    this.replyAddress = replyAddress;
    this.consumer = consumer;
    addRequestService(requestService);
    requestRegistry = new RequestRegistry();
    requestRegistry.setRequestTimeoutEnabled(true);
    requestRegistry.setRequestHandler(this);
    commandRegistry = new CommandRegistry("Router Context", null);
    createCommands();
    createDefaultExecutor();
    poller = new Poller(this);
    pollerService.execute(poller);
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
      AMQPMessage msg = new AMQPMessage();
      byte[] bytes = new byte[dos.getCount()];
      System.arraycopy(dos.getBuffer(), 0, bytes, 0, bytes.length);
      msg.addData(new Data(bytes));
      Properties prop = new Properties();
      prop.setReplyTo(replyAddress);
      msg.setProperties(prop);
      producer.send(msg);
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

  public void messageAvailable(Consumer consumer)
  {
    pollerService.execute(poller);
  }

  public void poll()
  {
    try
    {
      AMQPMessage msg = consumer.receiveNoWait(this);
      if (msg != null)
      {
        Data data = msg.getData().get(0);
        dis.reset();
        dis.setBuffer(data.getValue());
        Dumpable d = Dumpalizer.construct(dis, factory);
        if (d instanceof Reply)
          requestRegistry.setReply((Reply) d);
        else
          dispatch((Request) d);
        pollerService.execute(poller);
      }
    } catch (Exception e)
    {
      e.printStackTrace();
      close();
    }
  }

  public void close()
  {
    pollerService.shutdown();
    requestRegistry.cancelAllRequests(new TransportException("Request cancelled."));
    requestRegistry.close();
    TimerRegistry.Singleton().removeTimerListener(interval, this);
    try
    {
      consumer.close();
    } catch (Exception e)
    {
    }
    try
    {
      producer.close();
    } catch (Exception e)
    {
    }
    try
    {
      session.close();
    } catch (Exception e)
    {
    }
  }

  private static class Poller implements Runnable
  {
    EndpointImpl endpoint = null;

    private Poller(EndpointImpl endpoint)
    {
      this.endpoint = endpoint;
    }

    public void run()
    {
      endpoint.poll();
    }
  }
}