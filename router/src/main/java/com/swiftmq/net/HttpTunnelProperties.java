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

package com.swiftmq.net;

import com.swiftmq.tools.prop.StructuredProperties;
import com.swiftmq.tools.prop.SystemProperties;
import com.swiftmq.tools.sql.LikeComparator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.Properties;
import java.util.StringTokenizer;

// For documentation:
// HttpTunneling uses the CONNECT method. With this approach, a connection to a
// proxy is made, and after that a tunnel is established to the destination, tunneling starts. Often,
// the proxy allows only ports 443 and 563 for this tunnel, because this are the ports
// for SSL (SSL (https) will be tunneled exactly with this approach). Some proxies can
// be reconfigured, i. e. squid with "http_access deny CONNECT !Safe_ports" instead of
// "http_access deny CONNECT !SSL_ports". If this is not possible, the JMS listener must
// be defined on port 443 or 563.

public class HttpTunnelProperties implements Serializable
{
  public final static String PROP_FILENAME_PROP = "swiftmq.httptunnel.file";
  public final static String PROP_DEBUG = "swiftmq.httptunnel.debug";
  public final static String PROP_FILENAME = "/httptunnel.properties";
  public final static String PROP_PROXY_HOST = "proxy.host";
  public final static String PROP_PROXY_PORT = "proxy.port";
  public final static String PROP_PROXY_USERNAME = "proxy.username";
  public final static String PROP_PROXY_PASSWORD = "proxy.password";
  public final static String PROP_NO_PROXY_HOST = "noproxy.host";

  private String proxyHost = null;
  private int proxyPort = -1;
  private String username = null;
  private String password = null;
  private String[] excludeList = null;
  private boolean debug = false;

  private HttpTunnelProperties()
  {
    try
    {
      String sdebug = SystemProperties.get(PROP_DEBUG);
      debug = sdebug != null && sdebug.equals("true");
      InputStream in = null;
      String filename = SystemProperties.get(PROP_FILENAME_PROP);
      debug("Property filename = " + filename);
      if (filename != null)
        in = new FileInputStream(filename);
      else
        in = HttpTunnelProperties.class.getResourceAsStream(PROP_FILENAME);
      debug((in == null ? "No property file found, HttpTunneling disabled" : "Property file found, HttpTunneling enabled"));
      if (in == null)
        return;
      Properties prop = new Properties();
      prop.load(in);
      in.close();
      debug("Properties = " + prop);
      StructuredProperties sp = new StructuredProperties(prop);
      proxyHost = prop.getProperty(PROP_PROXY_HOST);
      proxyPort = Integer.parseInt(prop.getProperty(PROP_PROXY_PORT));
      excludeList = sp.getSectionElements(PROP_NO_PROXY_HOST);
      username = prop.getProperty(PROP_PROXY_USERNAME);
      password = prop.getProperty(PROP_PROXY_PASSWORD);
    } catch (Exception e)
    {
      System.err.println("Exception during load of httptunnel properties: " + e);
    }
  }

  private void debug(String msg)
  {
    if (debug)
      System.out.println("HttpTunneling: " + msg);
  }


  private static class InstanceHolder
  {
    public static HttpTunnelProperties instance = new HttpTunnelProperties();
  }

  public static HttpTunnelProperties getInstance()
  {
    return InstanceHolder.instance;
  }

  public void clear()
  {
    proxyHost = null;
    proxyPort = -1;
    username = null;
    password = null;
    excludeList = null;
  }

  public void reload(Properties prop)
  {
    if (debug)
      System.out.println("reload: " + prop);
    clear();
    if (prop != null && prop.size() > 0)
    {
      StructuredProperties sp = new StructuredProperties(prop);
      proxyHost = prop.getProperty(PROP_PROXY_HOST);
      proxyPort = Integer.parseInt(prop.getProperty(PROP_PROXY_PORT));
      excludeList = sp.getSectionElements(PROP_NO_PROXY_HOST);
      username = prop.getProperty(PROP_PROXY_USERNAME);
      password = prop.getProperty(PROP_PROXY_PASSWORD);
    }
  }

  public void reload(String filename) throws IOException
  {
    if (debug)
      System.out.println("reload from file: " + filename);
    FileInputStream in = new FileInputStream(filename);
    if (in == null)
      throw new FileNotFoundException(filename);
    Properties prop = new Properties();
    prop.load(in);
    reload(prop);
    in.close();
  }

  public boolean isProxy()
  {
    return proxyHost != null && proxyPort != -1;
  }

  public String getProxyHost()
  {
    return (proxyHost);
  }

  public int getProxyPort()
  {
    return (proxyPort);
  }

  public boolean isHostViaProxy(String hostname)
  {
    debug("Checking, if host '" + hostname + "' goes through the proxy ...");
    if (excludeList == null)
    {
      debug("Yes (no exclude list defined)");
      return true;
    }
    for (int i = 0; i < excludeList.length; i++)
    {
      if (LikeComparator.compare(hostname, excludeList[i], '\\'))
      {
        debug("No (matches for entry '" + excludeList[i] + "')");
        return false;
      }
    }
    debug("Yes (does not match any entry in the exclude list)");
    return true;
  }

  // Does the necessary initial handling for HTTP 1.1 proxy connect
  public void setupHttpProxy(String host, int port, InputStream socketInputStream, OutputStream socketOutputStream) throws IOException
  {
    StringBuffer b = new StringBuffer("CONNECT ");
    b.append(host);
    b.append(':');
    b.append(port);
    b.append(" HTTP/1.0\r\nUser-Agent: SwiftMQ JMS Enterprise Messaging System\r\n");
    if (username != null)
      b.append("Proxy-Authorization: Basic " + (Base64.getEncoder().encodeToString((username + ":" + password).getBytes()) + "\r\n"));
    b.append("\r\n");
    debug("Sending to proxy: \n" + b.toString());
    socketOutputStream.write(b.toString().getBytes());
    socketOutputStream.flush();
    String s = new String();
    int c;
    while ((c = socketInputStream.read()) != -1)
    {
      s += String.valueOf((char) c);
      if (s.endsWith("\r\n\r\n"))
        break;
    }
    debug("Receiving from proxy: \n" + s);
    StringTokenizer t = new StringTokenizer(s);
    t.nextToken(); // skip http-version
    String code = t.nextToken();
    if (code.equals("200"))
    {
      debug("Response from proxy is 200; tunneling starts ...");
      return;
    }
    debug("Response from proxy is != 200; throwing exception");
    throw new IOException("Error during proxy setup. Proxy responds with:\n" + s);
  }
}

