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

package com.swiftmq.impl.jms.standard;

import com.swiftmq.swiftlet.net.*;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.jms.smqp.*;
import com.swiftmq.tools.util.*;
import com.swiftmq.tools.dump.Dumpalizer;
import com.swiftmq.mgmt.*;

import java.io.*;

public class VersionSelector implements InboundHandler
{
  SwiftletContext ctx = null;
  Entity connectionEntity = null;
  SMQPFactory factory = new SMQPFactory();
  DataStreamInputStream dis = new DataStreamInputStream();
  VersionedJMSConnection jmsConnection = null;
  InboundHandler delegated = null;
  Connection connection = null;

  public VersionSelector(SwiftletContext ctx, Entity connectionEntity)
  {
    this.ctx = ctx;
    this.connectionEntity = connectionEntity;
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", toString() + "/created");
  }

  public VersionedJMSConnection getJmsConnection()
  {
    return jmsConnection;
  }

  public void dataAvailable(Connection connection, InputStream in) throws IOException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", toString() + "/dataAvailable...");
    if (delegated == null)
    {
      this.connection = connection;
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", toString() + "/dataAvailable/reading version request...");
      dis.setInputStream(in);
      SMQPVersionRequest request = (SMQPVersionRequest)factory.createDumpable(dis.readInt());
      request.readContent(dis);
      SMQPVersionReply reply = (SMQPVersionReply)request.createReply();
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", toString() + "/dataAvailable/version request: "+request);
      jmsConnection = ctx.jmsSwiftlet.createJMSConnection(request.getVersion(),connectionEntity,connection);
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", toString() + "/dataAvailable/jmsConnection: "+jmsConnection);
      if (jmsConnection != null)
      {
        try
        {
          connectionEntity.getProperty("version").setValue(new Integer(request.getVersion()));
        } catch (Exception e)
        {
        }
        delegated = jmsConnection.getInboundHandler();
        reply.setOk(true);
        jmsConnection.sendReply(reply);
      } else
      {
        reply.setOk(false);
        reply.setException(new Exception("Invalid version: "+request.getVersion()+", connection rejected!"));
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", toString() + "/dataAvailable/invalid version: "+request.getVersion());
        ctx.logSwiftlet.logError("sys$jms", toString() + "/JMS Client requests invalid Version: "+request.getVersion());
        sendReply(reply);
      }
    } else
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", toString() + "/dataAvailable/delegate");
      delegated.dataAvailable(connection,in);
    }
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", toString() + "/dataAvailable done");
  }

  private void sendReply(SMQPVersionReply reply) throws IOException
  {
    DataStreamOutputStream dos = new DataStreamOutputStream(connection.getOutputStream());
    Dumpalizer.dump(dos, reply);
    dos.flush();
    if (!reply.isOk())
    {
      ctx.timerSwiftlet.addInstantTimerListener(10000,new TimerListener()
      {
        public void performTimeAction()
        {
          ctx.networkSwiftlet.getConnectionManager().removeConnection(connection);
        }
      });
    }

  }

  public void close()
  {
    if (jmsConnection != null)
      jmsConnection.close();
  }

  public String toString()
  {
    return "VersionSelector";
  }
}
