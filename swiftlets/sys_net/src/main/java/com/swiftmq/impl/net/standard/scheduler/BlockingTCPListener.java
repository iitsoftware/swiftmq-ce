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

package com.swiftmq.impl.net.standard.scheduler;

import com.swiftmq.impl.net.standard.NetworkSwiftletImpl;
import com.swiftmq.impl.net.standard.TCPConnection;
import com.swiftmq.net.SocketFactory;
import com.swiftmq.net.SocketFactory2;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.net.ConnectionManager;
import com.swiftmq.swiftlet.net.ConnectionVetoException;
import com.swiftmq.swiftlet.net.ListenerMetaData;
import com.swiftmq.swiftlet.net.event.ConnectionListener;
import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Map;

public class BlockingTCPListener extends TCPListener
{
  SocketFactory socketFactory = null;
  NetworkSwiftletImpl networkSwiftlet = null;
  ThreadpoolSwiftlet threadpoolSwiftlet = null;
  LogSwiftlet logSwiftlet = null;
  TraceSwiftlet traceSwiftlet = null;
  TraceSpace traceSpace = null;
  ConnectionManager connectionManager = null;
  boolean closed = false;
  Listener listener = null;
  Map serverSockets = null;

  public BlockingTCPListener(ListenerMetaData metaData, SocketFactory socketFactory)
  {
    super(metaData);
    this.socketFactory = socketFactory;
    networkSwiftlet = (NetworkSwiftletImpl) SwiftletManager.getInstance().getSwiftlet("sys$net");
    threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
    logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
    traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
    connectionManager = networkSwiftlet.getConnectionManager();
    traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
    if (networkSwiftlet.isSetSocketOptions() && socketFactory instanceof SocketFactory2)
      ((SocketFactory2) socketFactory).setReceiveBufferSize(metaData.getInputBufferSize());
    if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/created");
  }

  /**
   * @throws IOException
   */
  public void start()
      throws IOException
  {
    if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/start");
    ServerSocket serverSocket = null;
    serverSockets = (Map) SwiftletManager.getInstance().getSurviveData("sys$net/serversockets");

    // Check if reuse server sockets and take the old one when found
    if (serverSockets != null)
    {
      if (traceSpace.enabled)
        traceSpace.trace("sys$net", toString() + "/checking if there is a prev. socket for port " + getMetaData().getPort());
      serverSocket = (ServerSocket) serverSockets.get(String.valueOf(getMetaData().getPort()));
      if (serverSocket != null)
      {
        if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/reuse " + serverSocket);
      }
    }

    // Here we may or may have not a server socket
    if (serverSocket == null)
    {
      if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/creating new server socket...");
      InetAddress bindAddress = getMetaData().getBindAddress();

      // On some platforms, SO_REUSEADDR isn't set for server sockets.
      // On Linux JDK1.3.0 it works only with java -classic.
      if (bindAddress == null)
        serverSocket = socketFactory.createServerSocket(getMetaData().getPort(), 512);
      else
        serverSocket = socketFactory.createServerSocket(getMetaData().getPort(), 512, bindAddress);
      if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/created: " + serverSocket);
    }
    // When re-using, the soTimeout needs to be set because the socket cannot be closed
    // and the accept needs a timeout.
    // Also, the server socket needs to be stored for later re-use
    if (serverSockets != null)
    {
      if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/storing for re-use: " + serverSocket);
      try
      {
        serverSocket.setSoTimeout(1000);
      } catch (SocketException e)
      {
      }
      serverSockets.put(String.valueOf(getMetaData().getPort()), serverSocket);
    }
    listener = new Listener(serverSocket);
    threadpoolSwiftlet.dispatchTask(listener);
  }

  public void close()
  {
    if (closed)
      return;
    if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/close");
    closed = true;
    listener.stop();
  }

  public String toString()
  {
    StringBuffer b = new StringBuffer();
    b.append("[BlockingTCPListener, ");
    b.append(super.toString());
    b.append(']');
    return b.toString();
  }

  private class Listener implements AsyncTask
  {
    ServerSocket serverSocket = null;
    ThreadPool myTP = null;
    boolean listenerClosed = false;

    Listener(ServerSocket serverSocket)
    {
      this.serverSocket = serverSocket;
      myTP = threadpoolSwiftlet.getPool(NetworkSwiftletImpl.TP_CONNHANDLER);
      if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/created");
    }

    public boolean isValid()
    {
      return !listenerClosed;
    }

    public String getDispatchToken()
    {
      return NetworkSwiftletImpl.TP_CONNHANDLER;
    }

    public String getDescription()
    {
      return BlockingTCPListener.this.toString();
    }

    public void stop()
    {
      if (listenerClosed)
        return;
      listenerClosed = true;
      if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/stop");
      if (serverSockets == null)
      {
        if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/closing server socket");
        try
        {
          serverSocket.close();
        } catch (Exception ignored)
        {
        }
      } else if (traceSpace.enabled)
        traceSpace.trace("sys$net", toString() + "/don't close server socket because it will be re-used");
    }

    public void run()
    {
      try
      {
        while (!listenerClosed)
        {
          Socket socket = null;
          try
          {
            if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/accept()...");
            socket = serverSocket.accept();
            try
            {
              socket.setTcpNoDelay(getMetaData().isUseTcpNoDelay());
            } catch (SocketException e)
            {
            }
            if (traceSpace.enabled)
              traceSpace.trace("sys$net", toString() + "/connection from " + socket.getInetAddress().getHostAddress());
            boolean allowed = true;
            if (networkSwiftlet.isDnsResolve())
              allowed = getMetaData().isConnectionAllowed(socket.getInetAddress().getHostName());
            if (allowed)
            {
              if (networkSwiftlet.isSetSocketOptions())
              {
                try
                {
                  socket.setSendBufferSize(getMetaData().getOutputBufferSize());
                } catch (SocketException e)
                {
                  if (traceSpace.enabled)
                    traceSpace.trace("sys$net", toString() + "/unable to perform 'socket.setSendBufferSize(" + getMetaData().getOutputBufferSize() + ")', exception: " + e);
                  logSwiftlet.logWarning(toString(), "/unable to perform 'socket.setSendBufferSize(" + getMetaData().getOutputBufferSize() + ")', exception: " + e);
                }
                if (socket.getReceiveBufferSize() != getMetaData().getInputBufferSize())
                {
                  try
                  {
                    socket.setReceiveBufferSize(getMetaData().getInputBufferSize());
                  } catch (SocketException e)
                  {
                    if (traceSpace.enabled)
                      traceSpace.trace("sys$net", toString() + "/unable to perform 'socket.setReceiveBufferSize(" + getMetaData().getInputBufferSize() + ")', exception: " + e);
                    logSwiftlet.logWarning(toString(), "/unable to perform 'socket.setReceiveBufferSize(" + getMetaData().getInputBufferSize() + ")', exception: " + e);
                  }
                }
              }
              if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/connection allowed, creating Handler");
              logSwiftlet.logInformation(toString(), "connection accepted: " + socket.getInetAddress().getHostAddress());
              try
              {
                socket.setSoTimeout(0); // Required for 1.4 since it inherits SO_TIMEOUT from the server socket
              } catch (SocketException e)
              {
              }
              TCPConnection connection = new TCPConnection(networkSwiftlet.isDnsResolve(), getMetaData().isUseTcpNoDelay(), socket);
              try
              {
                ConnectionListener connectionListener = getMetaData().getConnectionListener();
                connection.setConnectionListener(connectionListener);
                connection.setMetaData(getMetaData());
                connectionListener.connected(connection);
                connectionManager.addConnection(connection);
                myTP.dispatchTask(new BlockingHandler(connection));
              } catch (ConnectionVetoException cve)
              {
                connection.close();
                if (traceSpace.enabled)
                  traceSpace.trace("sys$net", toString() + "/ConnectionVetoException: " + cve.getMessage());
                logSwiftlet.logError(toString(), "ConnectionVetoException: " + cve.getMessage());
              }
            } else
            {
              if (traceSpace.enabled)
                traceSpace.trace("sys$net", toString() + "/connection NOT allowed, REJECTED: " + socket.getInetAddress().getHostAddress());
              logSwiftlet.logError(toString(), "connection NOT allowed, REJECTED: " + socket.getInetAddress().getHostAddress());
            }
          } catch (InterruptedIOException ioe)
          {
            if (traceSpace.enabled)
              traceSpace.trace("sys$net", toString() + "/interrupted by soTimeout, listenerClosed: " + listenerClosed);
          }
        }
      } catch (Exception e)
      {
        if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/Exception, EXITING: " + e);
        if (!listenerClosed)
          logSwiftlet.logWarning(toString(), "Exception, EXITING: " + e);
      }
      if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/return from run()");
    }

    public String toString()
    {
      return BlockingTCPListener.this.toString() + "/Listener";
    }
  }
}

