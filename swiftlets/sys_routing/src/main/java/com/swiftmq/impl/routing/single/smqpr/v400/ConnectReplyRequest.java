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

package com.swiftmq.impl.routing.single.smqpr.v400;

import com.swiftmq.impl.routing.single.smqpr.SMQRVisitor;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.ReplyRequest;
import com.swiftmq.tools.requestreply.RequestVisitor;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.*;

public class ConnectReplyRequest extends ReplyRequest
{
  String routerName = null;
  boolean authRequired = false;
  String crFactory = null;
  Serializable challenge = null;
  long keepAliveInterval = 0;

  public ConnectReplyRequest()
  {
    super(0, false);
  }

  public int getDumpId()
  {
    return SMQRFactory.CONNECT_REPREQ;
  }

  public void writeContent(DataOutput output) throws IOException
  {
    super.writeContent(output);
    output.writeUTF(routerName);
    output.writeBoolean(authRequired);
    if (authRequired)
    {
      output.writeUTF(crFactory);
      DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(dbos);
      oos.writeObject(challenge);
      oos.flush();
      oos.close();
      output.writeInt(dbos.getCount());
      output.write(dbos.getBuffer(), 0, dbos.getCount());
    }
    output.writeLong(keepAliveInterval);
  }

  public void readContent(DataInput input) throws IOException
  {
    super.readContent(input);
    routerName = input.readUTF();
    authRequired = input.readBoolean();
    if (authRequired)
    {
      crFactory = input.readUTF();
      byte b[] = new byte[input.readInt()];
      input.readFully(b);
      DataByteArrayInputStream dbis = new DataByteArrayInputStream(b);
      ObjectInputStream ois = new ObjectInputStream(dbis);
      try
      {
        challenge = (Serializable) ois.readObject();
      } catch (ClassNotFoundException e)
      {
        throw new IOException(e.toString());
      }
      ois.close();
    }
    keepAliveInterval = input.readLong();
  }

  protected Reply createReplyInstance()
  {
    return null;
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQRVisitor) visitor).handleRequest(this);
  }

  public String getRouterName()
  {
    return routerName;
  }

  public void setRouterName(String routerName)
  {
    this.routerName = routerName;
  }

  public boolean isAuthRequired()
  {
    return authRequired;
  }

  public void setAuthRequired(boolean authRequired)
  {
    this.authRequired = authRequired;
  }

  public String getCrFactory()
  {
    return crFactory;
  }

  public void setCrFactory(String crFactory)
  {
    this.crFactory = crFactory;
  }

  public Serializable getChallenge()
  {
    return challenge;
  }

  public void setChallenge(Serializable challenge)
  {
    this.challenge = challenge;
  }

  public long getKeepAliveInterval()
  {
    return keepAliveInterval;
  }

  public void setKeepAliveInterval(long keepAliveInterval)
  {
    this.keepAliveInterval = keepAliveInterval;
  }

  public String toString()
  {
    return "[ConnectReplyRequest " + super.toString() + ", routerName=" + routerName + ", authRequired=" + authRequired + ", crFactory=" + crFactory + ", challenge=" + challenge + ", keepAliveInterval=" + keepAliveInterval + "]";
  }
}
