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

package com.swiftmq.streamrepo;

import com.swiftmq.amqp.AMQPContext;
import com.swiftmq.amqp.v100.client.*;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AddressIF;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AmqpValue;
import com.swiftmq.amqp.v100.generated.messaging.message_format.ApplicationProperties;
import com.swiftmq.amqp.v100.generated.messaging.message_format.Properties;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.AMQPBoolean;
import com.swiftmq.amqp.v100.types.AMQPMap;
import com.swiftmq.amqp.v100.types.AMQPString;
import com.swiftmq.net.JSSESocketFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class AMQPRepo {
  static final String REPOQUEUE = "streamrepo";
  static final long TIMEOUT = 30000;
  static final String Q_MECHANISM = "mechanism";
  static final String Q_MAXFRAMESIZE = "maxframesize";
  static final String Q_IDLETIMEOUT = "idletimeout";
  Connection connection;
  Session session;
  Producer producer;
  Consumer consumer;
  AddressIF replyQueue;
  String routername;

  AMQPRepo(String url, String username, String password, String routername) throws Exception {
    this.routername = routername;
    try {
      connect(url, username, password);
      session = connection.createSession(100, 100);
      producer = session.createProducer(REPOQUEUE + '@' + routername, QoS.AT_MOST_ONCE);
      consumer = session.createConsumer(100, QoS.AT_MOST_ONCE);
      replyQueue = consumer.getRemoteAddress();
    } catch (Exception e) {
      try {
        if (connection != null)
          connection.close();
      } catch (Exception e1) {
      }
      throw e;
    }
  }

  private Map<String, String> getQueryMap(String query) {
    String[] params = query.split("&");
    Map<String, String> map = new HashMap<String, String>();
    for (String param : params) {
      String name = param.split("=")[0];
      String value = param.split("=")[1];
      map.put(name, value);
    }
    return map;
  }

  public void connect(String urlString, String username, String password) throws Exception {
    // parse url
    URL url = new URL(urlString.replaceFirst("amqp:", "http:").replaceFirst("amqps:", "https:"));
    String hostname = url.getHost();
    if (hostname == null)
      hostname = "localhost";
    int port = url.getPort();
    if (port == -1)
      port = urlString.startsWith("amqps:") ? 5671 : 5672;

    // create connection
    AMQPContext ctx = new AMQPContext(AMQPContext.CLIENT);
    if (username == null || username.trim().length() == 0)
      connection = new Connection(ctx, hostname, port, true);
    else
      connection = new Connection(ctx, hostname, port, username, password);
    if (urlString.startsWith("amqps:"))
      connection.setSocketFactory(new JSSESocketFactory());
    String query = url.getQuery();
    if (query != null) {
      Map<String, String> queryMap = getQueryMap(query);
      String v = queryMap.get(Q_MECHANISM);
      if (v != null)
        connection.setMechanism(v);
      v = queryMap.get(Q_MAXFRAMESIZE);
      if (v != null)
        connection.setMaxFrameSize(Long.parseLong(v));
      v = queryMap.get(Q_IDLETIMEOUT);
      if (v != null)
        connection.setIdleTimeout(Long.parseLong(v));

    }
    connection.connect();
  }

  private String loadFile(File f) throws Exception {
    BufferedReader reader = new BufferedReader(new FileReader(f));
    char[] buffer = new char[(int) f.length()];
    reader.read(buffer);
    reader.close();
    return new String(buffer);
  }

  AMQPRepo addDir(File dir, final String extension, String appname) throws Exception {
    File[] files = dir.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.endsWith(extension);
      }
    });
    for (int i = 0; i < files.length; i++)
      add(files[i], appname);
    return this;
  }

  AMQPRepo add(File file, String appname) throws Exception {
    String filename = file.getName();
    String content = loadFile(file);
    AMQPMessage request = new AMQPMessage();
    Map propMap = new HashMap();
    propMap.put(new AMQPString("app"), new AMQPString(appname));
    propMap.put(new AMQPString("file"), new AMQPString(filename));
    propMap.put(new AMQPString("operation"), new AMQPString("add"));
    request.setApplicationProperties(new ApplicationProperties(propMap));
    Properties properties = new Properties();
    properties.setReplyTo(replyQueue);
    request.setProperties(properties);
    request.setAmqpValue(new AmqpValue(new AMQPString(content)));
    producer.send(request);
    AMQPMessage reply = consumer.receive(TIMEOUT);
    if (reply == null)
      throw new Exception("Timeout occurred while waiting for a reply!");
    AMQPMap body = (AMQPMap) reply.getAmqpValue().getValue();
    boolean success = ((AMQPBoolean) (body.getValue().get(new AMQPString("success")))).getValue();
    if (success)
      System.out.println(filename + " added to repository " + appname);
    else {
      String result = ((AMQPString) (body.getValue().get(new AMQPString("result")))).getValue();
      System.out.println(result);
    }
    return this;
  }

  AMQPRepo removeDir(File dir, final String extension, String appname) throws Exception {
    File[] files = dir.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.endsWith(extension);
      }
    });
    for (int i = 0; i < files.length; i++)
      remove(files[i], appname);
    return this;
  }

  AMQPRepo remove(File file, String appname) throws Exception {
    String filename = file.getName();
    AMQPMessage request = new AMQPMessage();
    Map propMap = new HashMap();
    propMap.put(new AMQPString("app"), new AMQPString(appname));
    propMap.put(new AMQPString("file"), new AMQPString(filename));
    propMap.put(new AMQPString("operation"), new AMQPString("remove"));
    request.setApplicationProperties(new ApplicationProperties(propMap));
    Properties properties = new Properties();
    properties.setReplyTo(replyQueue);
    request.setProperties(properties);
    producer.send(request);
    AMQPMessage reply = consumer.receive(TIMEOUT);
    if (reply == null)
      throw new Exception("Timeout occurred while waiting for a reply!");
    AMQPMap body = (AMQPMap) reply.getAmqpValue().getValue();
    boolean success = ((AMQPBoolean) (body.getValue().get(new AMQPString("success")))).getValue();
    if (success)
      System.out.println(filename + " removed from repository " + appname);
    else {
      String result = ((AMQPString) (body.getValue().get(new AMQPString("result")))).getValue();
      System.out.println(result);
    }
    return this;
  }

  AMQPRepo remove(String appname) throws Exception {
    AMQPMessage request = new AMQPMessage();
    Map propMap = new HashMap();
    propMap.put(new AMQPString("app"), new AMQPString(appname));
    propMap.put(new AMQPString("operation"), new AMQPString("remove"));
    request.setApplicationProperties(new ApplicationProperties(propMap));
    Properties properties = new Properties();
    properties.setReplyTo(replyQueue);
    request.setProperties(properties);
    producer.send(request);
    AMQPMessage reply = consumer.receive(TIMEOUT);
    if (reply == null)
      throw new Exception("Timeout occurred while waiting for a reply!");
    AMQPMap body = (AMQPMap) reply.getAmqpValue().getValue();
    boolean success = ((AMQPBoolean) (body.getValue().get(new AMQPString("success")))).getValue();
    if (success)
      System.out.println("Removed repository " + appname);
    else {
      String result = ((AMQPString) (body.getValue().get(new AMQPString("result")))).getValue();
      System.out.println(result);
    }
    return this;
  }

  AMQPRepo list(String appname) throws Exception {
    AMQPMessage request = new AMQPMessage();
    Map propMap = new HashMap();
    propMap.put(new AMQPString("app"), new AMQPString(appname));
    propMap.put(new AMQPString("operation"), new AMQPString("list"));
    request.setApplicationProperties(new ApplicationProperties(propMap));
    Properties properties = new Properties();
    properties.setReplyTo(replyQueue);
    request.setProperties(properties);
    producer.send(request);
    AMQPMessage reply = consumer.receive(TIMEOUT);
    if (reply == null)
      throw new Exception("Timeout occurred while waiting for a reply!");
    AMQPMap body = (AMQPMap) reply.getAmqpValue().getValue();
    boolean success = ((AMQPBoolean) (body.getValue().get(new AMQPString("success")))).getValue();
    if (success) {
      System.out.println("Content of repository " + appname + ":");
      System.out.println();
      int n = Integer.parseInt(((AMQPString) (body.getValue().get(new AMQPString("nfiles")))).getValue());
      for (int i = 0; i < n; i++)
        System.out.println(((AMQPString) (body.getValue().get(new AMQPString("storetime" + i)))).getValue()
          + "\t" + ((AMQPString) (body.getValue().get(new AMQPString("file" + i)))).getValue());
      System.out.println();
    } else {
      String result = ((AMQPString) (body.getValue().get(new AMQPString("result")))).getValue();
      System.out.println(result);
    }
    return this;
  }

  void close() {
    try {
      if (connection != null)
        connection.close();
    } catch (Exception e) {
    }
  }

  public static void main(String[] args) {
    String url;
    String username;
    String password;
    String routername;
    String operation;
    String appname;
    String filename;
    String extension;
    if (args.length < 6) {
        System.err.println("Usage: repo <amqp-url> <username> <password> <routername> add|remove|list <reponame> [<streamfile>|(<streamdir> <extension>)]");
      System.exit(-1);
    }
    url = args[0];
    username = args[1];
    password = args[2];
    routername = args[3];
    operation = args[4];
    if (!(operation.equals("add") || operation.equals("remove") || operation.equals("list"))) {
      System.err.println("Operation must be: 'add', 'remove' or 'list'.");
      System.exit(-1);
    }
    appname = args[5];
    try {
      if (operation.equals("add")) {
        if (args.length < 7) {
          System.err.println("Missing: [<streamfile>|(<streamdir> <extension>)]");
          System.exit(-1);
        }
        filename = args[6];

        File file = new File(filename);
        if (!file.exists()) {
          System.err.println("File or directory '" + filename + "' does not exists.");
          System.exit(-1);
        }
        if (file.isDirectory()) {
          if (args.length != 8) {
            System.err.println("Missing '<extension>', e.g. '.js'.");
            System.exit(-1);
          }
          extension = args[7];
          new AMQPRepo(url, username, password, routername).addDir(file, extension, appname).close();
        } else
          new AMQPRepo(url, username, password, routername).add(file, appname).close();

      } else if (operation.equals("remove")) {
        if (args.length == 6) {
          new AMQPRepo(url, username, password, routername).remove(appname).close();
        } else {
          filename = args[6];
          File file = new File(filename);
          if (!file.exists()) {
            System.err.println("File or directory '" + filename + "' does not exists.");
            System.exit(-1);
          }
          if (file.isDirectory()) {
            if (args.length != 8) {
              System.err.println("Missing '<extension>', e.g. '.js'.");
              System.exit(-1);
            }
            extension = args[7];
            new AMQPRepo(url, username, password, routername).removeDir(file, extension, appname).close();
          } else
            new AMQPRepo(url, username, password, routername).remove(file, appname).close();
        }
      } else
        new AMQPRepo(url, username, password, routername).list(appname).close();
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println(e.getMessage());
      System.exit(-1);
    }
  }
}
