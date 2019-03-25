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

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.util.Hashtable;

public class JMSRepo {
    static final String REPOQUEUE = "streamrepo";
    static final long TIMEOUT = 30000;
    Connection connection;
    Session session;
    MessageProducer producer;
    MessageConsumer consumer;
    TemporaryQueue replyQueue;
    Queue repoQueue;
    String routername;

    JMSRepo(String url, String cf, String username, String password, String routername) throws Exception {
        this.routername = routername;
        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.swiftmq.jndi.InitialContextFactoryImpl");
        env.put(Context.PROVIDER_URL, url);
        try {
            InitialContext ctx = new InitialContext(env);
            try {
                connection = ((ConnectionFactory) ctx.lookup(cf)).createConnection(username, password);
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                repoQueue = session.createQueue(REPOQUEUE + '@' + routername);
                producer = session.createProducer(repoQueue);
                replyQueue = session.createTemporaryQueue();
                consumer = session.createConsumer(replyQueue);
                connection.start();
            } finally {
                ctx.close();
            }
        } catch (Exception e) {
            try {
                if (connection != null)
                    connection.close();
            } catch (JMSException e1) {
            }
            throw e;
        }
    }

    private String loadFile(File f) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(f));
        char[] buffer = new char[(int) f.length()];
        reader.read(buffer);
        reader.close();
        return new String(buffer);
    }

    JMSRepo addDir(File dir, final String extension, String appname) throws Exception {
        File[] files = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(extension);
            }
        });
        for (int i = 0; i < files.length; i++)
            add(files[i], appname);
        return this;
    }

    JMSRepo add(File file, String appname) throws Exception {
        String filename = file.getName();
        String content = loadFile(file);
        TextMessage request = session.createTextMessage();
        request.setStringProperty("app", appname);
        request.setStringProperty("file", filename);
        request.setStringProperty("operation", "add");
        request.setText(content);
        request.setJMSReplyTo(replyQueue);
        producer.send(request);
        MapMessage reply = (MapMessage) consumer.receive(TIMEOUT);
        if (reply == null)
            throw new Exception("Timeout occurred while waiting for a reply!");
        if (reply.getBoolean("success"))
            System.out.println(filename + " added to repository " + appname);
        else
            System.out.println(reply.getString("result"));
        return this;
    }

    JMSRepo removeDir(File dir, final String extension, String appname) throws Exception {
        File[] files = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(extension);
            }
        });
        for (int i = 0; i < files.length; i++)
            remove(files[i], appname);
        return this;
    }

    JMSRepo remove(File file, String appname) throws Exception {
        String filename = file.getName();
        Message request = session.createMessage();
        request.setStringProperty("app", appname);
        request.setStringProperty("file", filename);
        request.setStringProperty("operation", "remove");
        request.setJMSReplyTo(replyQueue);
        producer.send(request);
        MapMessage reply = (MapMessage) consumer.receive(TIMEOUT);
        if (reply == null)
            throw new Exception("Timeout occurred while waiting for a reply!");
        if (reply.getBoolean("success"))
            System.out.println(filename + " removed from repository " + appname);
        else
            System.out.println(reply.getString("result"));
        return this;
    }

    JMSRepo remove(String appname) throws Exception {
        Message request = session.createMessage();
        request.setStringProperty("app", appname);
        request.setStringProperty("operation", "remove");
        request.setJMSReplyTo(replyQueue);
        producer.send(request);
        MapMessage reply = (MapMessage) consumer.receive(TIMEOUT);
        if (reply == null)
            throw new Exception("Timeout occurred while waiting for a reply!");
        if (reply.getBoolean("success"))
            System.out.println("Removed repository " + appname);
        else
            System.out.println(reply.getString("result"));
        return this;
    }

    JMSRepo list(String appname) throws Exception {
        Message request = session.createMessage();
        request.setStringProperty("app", appname);
        request.setStringProperty("operation", "list");
        request.setJMSReplyTo(replyQueue);
        producer.send(request);
        MapMessage reply = (MapMessage) consumer.receive(TIMEOUT);
        if (reply == null)
            throw new Exception("Timeout occurred while waiting for a reply!");
        if (reply.getBoolean("success")) {
            System.out.println("Content of repository " + appname + ":");
            System.out.println();
            int n = Integer.parseInt(reply.getString("nfiles"));
            for (int i = 0; i < n; i++)
                System.out.println(reply.getString("storetime" + i) + "\t" + reply.getString("file" + i));
            System.out.println();
        } else
            System.out.println(reply.getString("result"));
        return this;
    }

    void close() {
        try {
            if (connection != null)
                connection.close();
        } catch (JMSException e) {
        }
    }

    public static void main(String[] args) {
        String url;
        String cf;
        String username;
        String password;
        String routername;
        String operation;
        String appname;
        String filename;
        String extension;
        if (args.length < 7) {
            System.err.println("Usage: repo <smqp-url> <connectionfactory> <username> <password> <routername> add|remove|list <reponame> [<streamfile>|(<streamdir> <extension>)]");
            System.exit(-1);
        }
        url = args[0];
        cf = args[1];
        username = args[2];
        password = args[3];
        routername = args[4];
        operation = args[5];
        if (!(operation.equals("add") || operation.equals("remove") || operation.equals("list"))) {
            System.err.println("Operation must be: 'add', 'remove' or 'list'.");
            System.exit(-1);
        }
        appname = args[6];
        try {
            if (operation.equals("add")) {
                if (args.length < 8) {
                    System.err.println("Missing: [<streamfile>|(<streamdir> <extension>)]");
                    System.exit(-1);
                }
                filename = args[7];

                File file = new File(filename);
                if (!file.exists()) {
                    System.err.println("File or directory '" + filename + "' does not exists.");
                    System.exit(-1);
                }
                if (file.isDirectory()) {
                    if (args.length != 9) {
                        System.err.println("Missing '<extension>', e.g. '.js'.");
                        System.exit(-1);
                    }
                    extension = args[8];
                    new JMSRepo(url, cf, username, password, routername).addDir(file, extension, appname).close();
                } else
                    new JMSRepo(url, cf, username, password, routername).add(file, appname).close();

            } else if (operation.equals("remove")) {
                if (args.length == 7) {
                    new JMSRepo(url, cf, username, password, routername).remove(appname).close();
                } else {
                    filename = args[7];
                    File file = new File(filename);
                    if (!file.exists()) {
                        System.err.println("File or directory '" + filename + "' does not exists.");
                        System.exit(-1);
                    }
                    if (file.isDirectory()) {
                        if (args.length != 9) {
                            System.err.println("Missing '<extension>', e.g. '.js'.");
                            System.exit(-1);
                        }
                        extension = args[8];
                        new JMSRepo(url, cf, username, password, routername).removeDir(file, extension, appname).close();
                    } else
                        new JMSRepo(url, cf, username, password, routername).remove(file, appname).close();
                }
            } else
                new JMSRepo(url, cf, username, password, routername).list(appname).close();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }
}
