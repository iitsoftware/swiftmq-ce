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

package com.swiftmq.swiftlet.net;

import com.swiftmq.net.protocol.ProtocolInputHandler;
import com.swiftmq.net.protocol.ProtocolOutputHandler;
import com.swiftmq.swiftlet.net.event.ConnectionListener;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An abstract connection.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public abstract class Connection
{
  protected boolean dnsResolve = true;
  long connectTime;
  ConnectionListener connectionListener;
  InboundHandler inboundHandler;
  ConnectionMetaData metaData = null;
  Object userObject = null;
  boolean markedForClose = false;
  boolean closed = false;
  ProtocolInputHandler protocolInputHandler = null;
  ProtocolOutputHandler protocolOutputHandler = null;
  AtomicBoolean inputActiveIndicator = new AtomicBoolean(false);

  /**
   * Constructs the Connection
   */
  protected Connection(boolean dnsResolve)
  {
    this.dnsResolve = dnsResolve;
    connectTime = System.currentTimeMillis();
  }

  /**
   * Set the connection listener.
   * Internal use only.
   *
   * @param connectionListener connection listener.
   */
  public void setConnectionListener(ConnectionListener connectionListener)
  {
    this.connectionListener = connectionListener;
  }

  /**
   * Set the connection meta data
   * Internal use only.
   *
   * @param metaData meta data.
   */
  public void setMetaData(ConnectionMetaData metaData)
  {
    this.metaData = metaData;
  }

  /**
   * Returns the connection meta data
   *
   * @returns connection meta data
   */
  public ConnectionMetaData getMetaData()
  {
    return metaData;
  }

  /**
   * Set a user object.
   * Can be used to assign user (Swiftlet) specific data to this connection.
   *
   * @param userObject user object.
   */
  public void setUserObject(Object userObject)
  {
    this.userObject = userObject;
  }

  /**
   * Returns the user object
   *
   * @return user object.
   */
  public Object getUserObject()
  {
    return userObject;
  }

  public ProtocolInputHandler getProtocolInputHandler()
  {
    return protocolInputHandler;
  }

  public void setProtocolInputHandler(ProtocolInputHandler protocolInputHandler)
  {
    this.protocolInputHandler = protocolInputHandler;
  }

  public ProtocolOutputHandler getProtocolOutputHandler()
  {
    return protocolOutputHandler;
  }

  public void setProtocolOutputHandler(ProtocolOutputHandler protocolOutputHandler)
  {
    this.protocolOutputHandler = protocolOutputHandler;
  }

  public AtomicBoolean getInputActiveIndicator()
  {
    return inputActiveIndicator;
  }

  /**
   * Returns the host name with which this connection is established to.
   *
   * @return host name (null for multicast connections).
   */
  public abstract String getHostname();

  /**
   * Set the connection inbound handler.
   * Internal use only.
   *
   * @param inboundHandler inbound handler.
   */
  public void setInboundHandler(InboundHandler inboundHandler)
  {
    // SBgen: Assign variable
    this.inboundHandler = inboundHandler;
  }

  /**
   * Returns the connection inbound handler.
   *
   * @return inbound handler.
   */
  public InboundHandler getInboundHandler()
  {
    // SBgen: Get variable
    return (inboundHandler);
  }

  /**
   * Returns the connection input stream.
   *
   * @return input stream.
   */
  public abstract InputStream getInputStream();

  /**
   * Returns the connection output stream.
   *
   * @return output stream.
   */
  public abstract OutputStream getOutputStream();

  /**
   * Returns the connect time.
   *
   * @return connect time.
   */
  public long getConnectTime()
  {
    // SBgen: Get variable
    return (connectTime);
  }

  /**
   * Mark this connection for close.
   * Internal use only.
   */
  public void setMarkedForClose()
  {
    markedForClose = true;
  }

  /**
   * Returns whether this connection is marked for close.
   *
   * @return true/false.
   */
  public boolean isMarkedForClose()
  {
    return markedForClose;
  }

  /**
   * Returns whether this connection is closed.
   *
   * @return true/false.
   */
  public boolean isClosed()
  {
    return closed;
  }

  /**
   * Closes this connection.
   * Internal use only.
   */
  public void close()
  {
    closed = true;
    connectionListener.disconnected(this);
  }
}

