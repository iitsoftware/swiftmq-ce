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

package com.swiftmq.jms.smqp.v630;

/** SMQP-Protocol Version 630, Class: RouterConnectReply
 *  Automatically generated, don't change!
 *  Generation Date: Thu Aug 30 17:17:54 CEST 2007
 *  (c) 2007, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.tools.requestreply.ReplyNE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RouterConnectReply extends ReplyNE
{
  private String routerName;
  private boolean authRequired;
  private long keepaliveInterval;

  public RouterConnectReply(String routerName, boolean authRequired, long keepaliveInterval)
  {
    this.routerName = routerName;
    this.authRequired = authRequired;
    this.keepaliveInterval = keepaliveInterval;
  }

  protected RouterConnectReply()
  {
  }

  public void setRouterName(String routerName)
  {
    this.routerName = routerName;
  }

  public String getRouterName()
  {
    return routerName;
  }

  public void setAuthRequired(boolean authRequired)
  {
    this.authRequired = authRequired;
  }

  public boolean isAuthRequired()
  {
    return authRequired;
  }

  public void setKeepaliveInterval(long keepaliveInterval)
  {
    this.keepaliveInterval = keepaliveInterval;
  }

  public long getKeepaliveInterval()
  {
    return keepaliveInterval;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_ROUTERCONNECT_REP;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    if (routerName != null)
    {
      out.writeBoolean(true);
      SMQPUtil.write(routerName, out);
    } else
      out.writeBoolean(false);
    SMQPUtil.write(authRequired, out);
    SMQPUtil.write(keepaliveInterval, out);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    boolean routerName_set = in.readBoolean();
    if (routerName_set)
      routerName = SMQPUtil.read(routerName, in);
    authRequired = SMQPUtil.read(authRequired, in);
    keepaliveInterval = SMQPUtil.read(keepaliveInterval, in);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[v630/RouterConnectReply, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("routerName=");
    _b.append(routerName);
    _b.append(", ");
    _b.append("authRequired=");
    _b.append(authRequired);
    _b.append(", ");
    _b.append("keepaliveInterval=");
    _b.append(keepaliveInterval);
    _b.append("]");
    return _b.toString();
  }
}
