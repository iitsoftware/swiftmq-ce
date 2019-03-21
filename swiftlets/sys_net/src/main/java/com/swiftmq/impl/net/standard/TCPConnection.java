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

package com.swiftmq.impl.net.standard;

import com.swiftmq.impl.net.standard.scheduler.BlockingHandler;
import com.swiftmq.swiftlet.net.Connection;
import com.swiftmq.swiftlet.net.ConnectionMetaData;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;

public class TCPConnection extends Connection
{
  Socket socket;
  InputStream in;
  OutputStream out;
  String hostname = null;
  int port = 0;
  int bufferSize;
  BlockingHandler handler = null;

  public TCPConnection(boolean dnsResolve, boolean useTcpNoDelay, Socket socket) throws IOException
  {
    super(dnsResolve);
    // SBgen: Assign variable
    this.socket = socket;
    try
    {
      this.socket.setTcpNoDelay(useTcpNoDelay);
    } catch (SocketException e)
    {
    }
    in = new CountableBufferedInputStream(socket.getInputStream());
  }

  private void determineHostnamePort()
  {
    if (hostname != null)
      return;
    try
    {
      hostname = dnsResolve ? socket.getInetAddress().getHostName() : socket.getInetAddress().getHostAddress();
      port = socket.getPort();
    } catch (Exception e)
    {
      hostname = null;
      port = -1;
    }
  }

  public void setHandler(BlockingHandler handler)
  {
    this.handler = handler;
  }

  public synchronized void setMetaData(ConnectionMetaData meta)
  {
    super.setMetaData(meta);
    try
    {
      setProtocolOutputHandler(meta.createProtocolOutputHandler());
      out = new CountableBufferedOutputStream(getProtocolOutputHandler(), socket.getOutputStream());
    } catch (Exception e)
    {
    }
  }

  public Socket getSocket()
  {
    return socket;
  }

  public String getHostname()
  {
    determineHostnamePort();
    if (hostname == null)
      return "unresolvable";
    return hostname;
  }

  public int getPort()
  {
    determineHostnamePort();
    return port;
  }

  public InputStream getInputStream()
  {
    return in;
  }

  public OutputStream getOutputStream()
  {
    return out;
  }

  public void close()
  {
    if (isClosed())
      return;
    super.close();
    // to ensure that a connector reconnects while an old handler still listens on an input stream
    if (handler != null)
      handler.internalClose();
    try
    {
      socket.shutdownInput();
    } catch (Exception ignored)
    {
    }
    try
    {
      socket.shutdownOutput();
    } catch (Exception ignored)
    {
    }
    try
    {
      socket.close();
    } catch (Exception ignored)
    {
    }
  }

  public String toString()
  {
    return getHostname() + ":" + getPort();
  }
}

